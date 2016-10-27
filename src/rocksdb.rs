// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


use libc::{self, c_int, c_void, size_t};

use rocksdb_ffi::{self, DBWriteBatch, DBCFHandle, DBInstance};
use rocksdb_options::{Options, ReadOptions, UnsafeSnap, WriteOptions, FlushOptions};
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::ffi::{CStr, CString};
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::ptr;
use std::slice;
use std::str::from_utf8;

const DEFAULT_COLUMN_FAMILY: &'static str = "default";

pub struct CFHandle {
    inner: *mut DBCFHandle,
}

impl Drop for CFHandle {
    fn drop(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_column_family_handle_destroy(self.inner);
        }
    }
}

pub struct DB {
    inner: *mut DBInstance,
    cfs: BTreeMap<String, CFHandle>,
    path: String,
}

unsafe impl Send for DB {}
unsafe impl Sync for DB {}

pub struct WriteBatch {
    inner: *mut DBWriteBatch,
}

pub struct Snapshot<'a> {
    db: &'a DB,
    snap: UnsafeSnap,
}

// We need to find a better way to add a lifetime in here.
#[allow(dead_code)]
pub struct DBIterator<'a> {
    db: &'a DB,
    readopts: ReadOptions,
    inner: *mut rocksdb_ffi::DBIterator,
}

pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}

impl<'a> From<&'a [u8]> for SeekKey<'a> {
    fn from(bs: &'a [u8]) -> SeekKey {
        SeekKey::Key(bs)
    }
}

impl<'a> DBIterator<'a> {
    pub fn new(db: &'a DB, readopts: ReadOptions) -> DBIterator<'a> {
        unsafe {
            let iterator = rocksdb_ffi::rocksdb_create_iterator(db.inner, readopts.get_inner());

            DBIterator {
                db: db,
                readopts: readopts,
                inner: iterator,
            }
        }
    }

    pub fn seek(&mut self, key: SeekKey) -> bool {
        unsafe {
            match key {
                SeekKey::Start => rocksdb_ffi::rocksdb_iter_seek_to_first(self.inner),
                SeekKey::End => rocksdb_ffi::rocksdb_iter_seek_to_last(self.inner),
                SeekKey::Key(key) => {
                    rocksdb_ffi::rocksdb_iter_seek(self.inner, key.as_ptr(), key.len() as size_t)
                }
            }
        }
        self.valid()
    }

    pub fn prev(&mut self) -> bool {
        unsafe {
            rocksdb_ffi::rocksdb_iter_prev(self.inner);
        }
        self.valid()
    }

    pub fn next(&mut self) -> bool {
        unsafe {
            rocksdb_ffi::rocksdb_iter_next(self.inner);
        }
        self.valid()
    }

    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        let mut key_len: size_t = 0;
        let key_len_ptr: *mut size_t = &mut key_len;
        unsafe {
            let key_ptr = rocksdb_ffi::rocksdb_iter_key(self.inner, key_len_ptr);
            slice::from_raw_parts(key_ptr, key_len as usize)
        }
    }

    pub fn value(&self) -> &[u8] {
        assert!(self.valid());
        let mut val_len: size_t = 0;
        let val_len_ptr: *mut size_t = &mut val_len;
        unsafe {
            let val_ptr = rocksdb_ffi::rocksdb_iter_value(self.inner, val_len_ptr);
            slice::from_raw_parts(val_ptr, val_len as usize)
        }
    }

    pub fn kv(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.valid() {
            Some((self.key().to_vec(), self.value().to_vec()))
        } else {
            None
        }
    }

    pub fn valid(&self) -> bool {
        unsafe { rocksdb_ffi::rocksdb_iter_valid(self.inner) }
    }

    pub fn new_cf(db: &'a DB, cf_handle: &CFHandle, readopts: ReadOptions) -> DBIterator<'a> {
        unsafe {
            let iterator = rocksdb_ffi::rocksdb_create_iterator_cf(db.inner,
                                                                   readopts.get_inner(),
                                                                   cf_handle.inner);
            DBIterator {
                db: db,
                readopts: readopts,
                inner: iterator,
            }
        }
    }
}

pub type Kv = (Vec<u8>, Vec<u8>);

impl<'b, 'a> Iterator for &'b mut DBIterator<'a> {
    type Item = Kv;

    fn next(&mut self) -> Option<Kv> {
        let kv = self.kv();
        if kv.is_some() {
            DBIterator::next(self);
        }
        kv
    }
}

impl<'a> Drop for DBIterator<'a> {
    fn drop(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_iter_destroy(self.inner);
        }
    }
}

impl<'a> Snapshot<'a> {
    pub fn new(db: &DB) -> Snapshot {
        unsafe {
            Snapshot {
                db: db,
                snap: db.unsafe_snap(),
            }
        }
    }

    pub fn iter(&self) -> DBIterator {
        let readopts = ReadOptions::new();
        self.iter_opt(readopts)
    }

    pub fn iter_opt(&self, mut opt: ReadOptions) -> DBIterator {
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(self.db, opt)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, String> {
        let mut readopts = ReadOptions::new();
        unsafe {
            readopts.set_snapshot(&self.snap);
        }
        self.db.get_opt(key, &readopts)
    }

    pub fn get_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<Option<DBVector>, String> {
        let mut readopts = ReadOptions::new();
        unsafe {
            readopts.set_snapshot(&self.snap);
        }
        self.db.get_cf_opt(cf, key, &readopts)
    }
}

impl<'a> Drop for Snapshot<'a> {
    fn drop(&mut self) {
        unsafe { self.db.release_snap(&self.snap) }
    }
}

// This is for the DB and write batches to share the same API
pub trait Writable {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String>;
    fn put_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String>;
    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), String>;
    fn merge_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String>;
    fn delete(&self, key: &[u8]) -> Result<(), String>;
    fn delete_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<(), String>;
}

/// A range of keys, `start_key` is included, but not `end_key`.
///
/// You should make sure `end_key` is not less than `start_key`.
pub struct Range<'a> {
    start_key: &'a [u8],
    end_key: &'a [u8],
}

impl<'a> Range<'a> {
    pub fn new(start_key: &'a [u8], end_key: &'a [u8]) -> Range<'a> {
        assert!(start_key <= end_key);
        Range {
            start_key: start_key,
            end_key: end_key,
        }
    }
}

impl DB {
    pub fn open_default(path: &str) -> Result<DB, String> {
        let mut opts = Options::new();
        opts.create_if_missing(true);
        DB::open(&opts, path)
    }

    pub fn open(opts: &Options, path: &str) -> Result<DB, String> {
        DB::open_cf(opts, path, &[], &[])
    }

    pub fn open_cf(opts: &Options,
                   path: &str,
                   cfs: &[&str],
                   cf_opts: &[&Options])
                   -> Result<DB, String> {
        let cpath = match CString::new(path.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err("Failed to convert path to CString when opening rocksdb".to_owned())
            }
        };
        if let Err(e) = fs::create_dir_all(&Path::new(path)) {
            return Err(format!("Failed to create rocksdb directory: \
                                src/rocksdb.rs:                              \
                                {:?}",
                               e));
        }

        if cfs.len() != cf_opts.len() {
            return Err(format!("cfs.len() and cf_opts.len() not match."));
        }

        let mut cfs_v = cfs.to_vec();
        let mut cf_opts_v = cf_opts.to_vec();
        // Always open the default column family
        if !cfs_v.contains(&DEFAULT_COLUMN_FAMILY) {
            cfs_v.push(DEFAULT_COLUMN_FAMILY);
            cf_opts_v.push(opts);
        }

        // We need to store our CStrings in an intermediate vector
        // so that their pointers remain valid.
        let c_cfs: Vec<CString> = cfs_v.iter()
            .map(|cf| CString::new(cf.as_bytes()).unwrap())
            .collect();

        let cfnames: Vec<*const _> = c_cfs.iter()
            .map(|cf| cf.as_ptr())
            .collect();

        // These handles will be populated by DB.
        let cfhandles: Vec<_> = cfs_v.iter()
            .map(|_| 0 as *mut DBCFHandle)
            .collect();

        let cfopts: Vec<_> = cf_opts_v.iter()
            .map(|x| x.inner as *const rocksdb_ffi::DBOptions)
            .collect();

        let db = unsafe {
            ffi_try!(rocksdb_open_column_families(opts.inner,
                                                  cpath.as_ptr(),
                                                  cfs_v.len() as c_int,
                                                  cfnames.as_ptr(),
                                                  cfopts.as_ptr(),
                                                  cfhandles.as_ptr()))
        };

        for handle in &cfhandles {
            if handle.is_null() {
                return Err("Received null column family handle from DB.".to_owned());
            }
        }

        let mut cf_map = BTreeMap::new();
        for (n, h) in cfs_v.iter().zip(cfhandles) {
            cf_map.insert((*n).to_owned(), CFHandle { inner: h });
        }

        if db.is_null() {
            return Err("Could not initialize database.".to_owned());
        }

        Ok(DB {
            inner: db,
            cfs: cf_map,
            path: path.to_owned(),
        })
    }

    pub fn destroy(opts: &Options, path: &str) -> Result<(), String> {
        let cpath = CString::new(path.as_bytes()).unwrap();
        unsafe {
            ffi_try!(rocksdb_destroy_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub fn repair(opts: Options, path: &str) -> Result<(), String> {
        let cpath = CString::new(path.as_bytes()).unwrap();
        unsafe {
            ffi_try!(rocksdb_repair_db(opts.inner, cpath.as_ptr()));
        }
        Ok(())
    }

    pub fn list_column_families(opts: &Options, path: &str) -> Result<Vec<String>, String> {
        let cpath = match CString::new(path.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err("Failed to convert path to CString when list \
                            column families"
                    .to_owned())
            }
        };

        let mut cfs: Vec<String> = vec![];
        unsafe {
            let mut lencf: size_t = 0;
            let list =
                ffi_try!(rocksdb_list_column_families(opts.inner, cpath.as_ptr(), &mut lencf));
            let list_cfs = slice::from_raw_parts(list, lencf);
            for &cf_name in list_cfs {
                let cf = match CStr::from_ptr(cf_name).to_owned().into_string() {
                    Ok(s) => s,
                    Err(e) => return Err(format!("invalid utf8 bytes: {:?}", e)),
                };
                cfs.push(cf);
            }
            rocksdb_ffi::rocksdb_list_column_families_destroy(list, lencf);
        }

        Ok(cfs)
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn write_opt(&self, batch: &WriteBatch, writeopts: &WriteOptions) -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_write(self.inner, writeopts.inner, batch.inner));
        }
        Ok(())
    }

    pub fn write(&self, batch: &WriteBatch) -> Result<(), String> {
        self.write_opt(batch, &WriteOptions::new())
    }

    pub fn write_without_wal(&self, batch: &WriteBatch) -> Result<(), String> {
        let mut wo = WriteOptions::new();
        wo.disable_wal(true);
        self.write_opt(batch, &wo)
    }

    pub fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<DBVector>, String> {
        unsafe {
            let val_len: size_t = 0;
            let val_len_ptr = &val_len as *const size_t;
            let val = ffi_try!(rocksdb_get(self.inner,
                                           readopts.get_inner(),
                                           key.as_ptr(),
                                           key.len() as size_t,
                                           val_len_ptr));
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, String> {
        self.get_opt(key, &ReadOptions::new())
    }

    pub fn get_cf_opt(&self,
                      cf: &CFHandle,
                      key: &[u8],
                      readopts: &ReadOptions)
                      -> Result<Option<DBVector>, String> {
        unsafe {
            let val_len: size_t = 0;
            let val_len_ptr = &val_len as *const size_t;
            let val = ffi_try!(rocksdb_get_cf(self.inner,
                                              readopts.get_inner(),
                                              cf.inner,
                                              key.as_ptr(),
                                              key.len() as size_t,
                                              val_len_ptr));
            if val.is_null() {
                Ok(None)
            } else {
                Ok(Some(DBVector::from_c(val, val_len)))
            }
        }
    }

    pub fn get_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<Option<DBVector>, String> {
        self.get_cf_opt(cf, key, &ReadOptions::new())
    }

    pub fn create_cf(&mut self, name: &str, opts: &Options) -> Result<&CFHandle, String> {
        let cname = match CString::new(name.as_bytes()) {
            Ok(c) => c,
            Err(_) => {
                return Err("Failed to convert path to CString when opening rocksdb".to_owned())
            }
        };
        let cname_ptr = cname.as_ptr();
        unsafe {
            let cf_handler =
                ffi_try!(rocksdb_create_column_family(self.inner, opts.inner, cname_ptr));
            let handle = CFHandle { inner: cf_handler };
            Ok(match self.cfs.entry(name.to_owned()) {
                Entry::Occupied(mut e) => {
                    e.insert(handle);
                    e.into_mut()
                }
                Entry::Vacant(e) => e.insert(handle),
            })
        }
    }

    pub fn drop_cf(&mut self, name: &str) -> Result<(), String> {
        let cf = self.cfs.remove(name);
        if cf.is_none() {
            return Err(format!("Invalid column family: {}", name).clone());
        }

        unsafe {
            ffi_try!(rocksdb_drop_column_family(self.inner, cf.unwrap().inner));
        }

        Ok(())
    }

    pub fn cf_handle(&self, name: &str) -> Option<&CFHandle> {
        self.cfs.get(name)
    }

    /// get all column family names, including 'default'.
    pub fn cf_names(&self) -> Vec<&str> {
        self.cfs.iter().map(|(k, _)| k.as_str()).collect()
    }

    pub fn iter(&self) -> DBIterator {
        let opts = ReadOptions::new();
        self.iter_opt(opts)
    }

    pub fn iter_opt(&self, opt: ReadOptions) -> DBIterator {
        DBIterator::new(&self, opt)
    }

    pub fn iter_cf(&self, cf_handle: &CFHandle) -> DBIterator {
        let opts = ReadOptions::new();
        DBIterator::new_cf(self, cf_handle, opts)
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self)
    }

    pub unsafe fn unsafe_snap(&self) -> UnsafeSnap {
        UnsafeSnap::new(self.inner)
    }

    pub unsafe fn release_snap(&self, snap: &UnsafeSnap) {
        rocksdb_ffi::rocksdb_release_snapshot(self.inner, snap.get_inner())
    }

    pub fn put_opt(&self,
                   key: &[u8],
                   value: &[u8],
                   writeopts: &WriteOptions)
                   -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_put(self.inner,
                                 writeopts.inner,
                                 key.as_ptr(),
                                 key.len() as size_t,
                                 value.as_ptr(),
                                 value.len() as size_t));
            Ok(())
        }
    }

    pub fn put_cf_opt(&self,
                      cf: &CFHandle,
                      key: &[u8],
                      value: &[u8],
                      writeopts: &WriteOptions)
                      -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_put_cf(self.inner,
                                    writeopts.inner,
                                    cf.inner,
                                    key.as_ptr(),
                                    key.len() as size_t,
                                    value.as_ptr(),
                                    value.len() as size_t));
            Ok(())
        }
    }
    pub fn merge_opt(&self,
                     key: &[u8],
                     value: &[u8],
                     writeopts: &WriteOptions)
                     -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_merge(self.inner,
                                   writeopts.inner,
                                   key.as_ptr(),
                                   key.len() as size_t,
                                   value.as_ptr(),
                                   value.len() as size_t));
            Ok(())
        }
    }
    fn merge_cf_opt(&self,
                    cf: &CFHandle,
                    key: &[u8],
                    value: &[u8],
                    writeopts: &WriteOptions)
                    -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_merge_cf(self.inner,
                                      writeopts.inner,
                                      cf.inner,
                                      key.as_ptr(),
                                      key.len() as size_t,
                                      value.as_ptr(),
                                      value.len() as size_t));
            Ok(())
        }
    }
    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_delete(self.inner,
                                    writeopts.inner,
                                    key.as_ptr(),
                                    key.len() as size_t));
            Ok(())
        }
    }

    fn delete_cf_opt(&self,
                     cf: &CFHandle,
                     key: &[u8],
                     writeopts: &WriteOptions)
                     -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_delete_cf(self.inner,
                                       writeopts.inner,
                                       cf.inner,
                                       key.as_ptr(),
                                       key.len() as size_t));
            Ok(())
        }
    }

    /// Flush all memtable data.
    ///
    /// Due to lack of abi, only default cf is supported.
    ///
    /// If sync, the flush will wait until the flush is done.
    pub fn flush(&self, sync: bool) -> Result<(), String> {
        unsafe {
            let mut opts = FlushOptions::new();
            opts.set_wait(sync);
            ffi_try!(rocksdb_flush(self.inner, opts.inner));
            Ok(())
        }
    }

    /// Return the approximate file system space used by keys in each ranges.
    ///
    /// Note that the returned sizes measure file system space usage, so
    /// if the user data compresses by a factor of ten, the returned
    /// sizes will be one-tenth the size of the corresponding user data size.
    ///
    /// Due to lack of abi, only data flushed to disk is taken into account.
    pub fn get_approximate_sizes(&self, ranges: &[Range]) -> Vec<u64> {
        self.get_approximate_sizes_cfopt(None, ranges)
    }

    pub fn get_approximate_sizes_cf(&self, cf: &CFHandle, ranges: &[Range]) -> Vec<u64> {
        self.get_approximate_sizes_cfopt(Some(cf), ranges)
    }

    fn get_approximate_sizes_cfopt(&self, cf: Option<&CFHandle>, ranges: &[Range]) -> Vec<u64> {
        let start_keys: Vec<*const u8> = ranges.iter()
            .map(|x| x.start_key.as_ptr())
            .collect();
        let start_key_lens: Vec<_> = ranges.iter()
            .map(|x| x.start_key.len())
            .collect();
        let end_keys: Vec<*const u8> = ranges.iter()
            .map(|x| x.end_key.as_ptr())
            .collect();
        let end_key_lens: Vec<_> = ranges.iter()
            .map(|x| x.end_key.len())
            .collect();
        let mut sizes: Vec<u64> = vec![0; ranges.len()];
        let (n, start_key_ptr, start_key_len_ptr, end_key_ptr, end_key_len_ptr, size_ptr) =
            (ranges.len() as i32,
             start_keys.as_ptr(),
             start_key_lens.as_ptr(),
             end_keys.as_ptr(),
             end_key_lens.as_ptr(),
             sizes.as_mut_ptr());
        match cf {
            None => unsafe {
                rocksdb_ffi::rocksdb_approximate_sizes(self.inner,
                                                       n,
                                                       start_key_ptr,
                                                       start_key_len_ptr,
                                                       end_key_ptr,
                                                       end_key_len_ptr,
                                                       size_ptr)
            },
            Some(cf) => unsafe {
                rocksdb_ffi::rocksdb_approximate_sizes_cf(self.inner,
                                                          cf.inner,
                                                          n,
                                                          start_key_ptr,
                                                          start_key_len_ptr,
                                                          end_key_ptr,
                                                          end_key_len_ptr,
                                                          size_ptr)
            },
        }
        sizes
    }

    pub fn compact_range(&self, start_key: Option<&[u8]>, end_key: Option<&[u8]>) {
        unsafe {
            let (start, s_len) = start_key.map_or((ptr::null(), 0), |k| (k.as_ptr(), k.len()));
            let (end, e_len) = end_key.map_or((ptr::null(), 0), |k| (k.as_ptr(), k.len()));
            rocksdb_ffi::rocksdb_compact_range(self.inner, start, s_len, end, e_len);
        }
    }

    pub fn compact_range_cf(&self,
                            cf: &CFHandle,
                            start_key: Option<&[u8]>,
                            end_key: Option<&[u8]>) {
        unsafe {
            let (start, s_len) = start_key.map_or((ptr::null(), 0), |k| (k.as_ptr(), k.len()));
            let (end, e_len) = end_key.map_or((ptr::null(), 0), |k| (k.as_ptr(), k.len()));
            rocksdb_ffi::rocksdb_compact_range_cf(self.inner, cf.inner, start, s_len, end, e_len);
        }
    }

    pub fn delete_file_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_delete_file_in_range(self.inner,
                                                  start_key.as_ptr(),
                                                  start_key.len() as size_t,
                                                  end_key.as_ptr(),
                                                  end_key.len() as size_t));
            Ok(())
        }
    }

    pub fn delete_file_in_range_cf(&self,
                                   cf: &CFHandle,
                                   start_key: &[u8],
                                   end_key: &[u8])
                                   -> Result<(), String> {
        unsafe {
            ffi_try!(rocksdb_delete_file_in_range_cf(self.inner,
                                                     cf.inner,
                                                     start_key.as_ptr(),
                                                     start_key.len() as size_t,
                                                     end_key.as_ptr(),
                                                     end_key.len() as size_t));
            Ok(())
        }
    }

    pub fn get_property_value(&self, name: &str) -> Option<String> {
        self.get_property_value_cf_opt(None, name)
    }

    pub fn get_property_value_cf(&self, cf: &CFHandle, name: &str) -> Option<String> {
        self.get_property_value_cf_opt(Some(cf), name)
    }

    /// Return the int property in rocksdb.
    /// Return None if the property not exists or not int type.
    pub fn get_property_int(&self, name: &str) -> Option<u64> {
        self.get_property_int_cf_opt(None, name)
    }

    pub fn get_property_int_cf(&self, cf: &CFHandle, name: &str) -> Option<u64> {
        self.get_property_int_cf_opt(Some(cf), name)
    }

    fn get_property_value_cf_opt(&self, cf: Option<&CFHandle>, name: &str) -> Option<String> {
        unsafe {
            let prop_name = CString::new(name).unwrap();

            let value = match cf {
                None => rocksdb_ffi::rocksdb_property_value(self.inner, prop_name.as_ptr()),
                Some(cf) => {
                    rocksdb_ffi::rocksdb_property_value_cf(self.inner, cf.inner, prop_name.as_ptr())
                }
            };

            if value.is_null() {
                return None;
            }

            // Must valid UTF-8 format.
            let s = CStr::from_ptr(value).to_str().unwrap().to_owned();
            libc::free(value as *mut c_void);
            Some(s)
        }
    }

    fn get_property_int_cf_opt(&self, cf: Option<&CFHandle>, name: &str) -> Option<u64> {
        // Rocksdb guarantees that the return property int
        // value is u64 if exists.
        if let Some(value) = self.get_property_value_cf_opt(cf, name) {
            if let Ok(num) = value.as_str().parse::<u64>() {
                return Some(num);
            }
        }

        None
    }
}

impl Writable for DB {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.put_opt(key, value, &WriteOptions::new())
    }

    fn put_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.put_cf_opt(cf, key, value, &WriteOptions::new())
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.merge_opt(key, value, &WriteOptions::new())
    }

    fn merge_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        self.merge_cf_opt(cf, key, value, &WriteOptions::new())
    }

    fn delete(&self, key: &[u8]) -> Result<(), String> {
        self.delete_opt(key, &WriteOptions::new())
    }

    fn delete_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
        self.delete_cf_opt(cf, key, &WriteOptions::new())
    }
}

impl Default for WriteBatch {
    fn default() -> WriteBatch {
        WriteBatch { inner: unsafe { rocksdb_ffi::rocksdb_writebatch_create() } }
    }
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch::default()
    }

    pub fn count(&self) -> usize {
        unsafe { rocksdb_ffi::rocksdb_writebatch_count(self.inner) as usize }
    }

    pub fn is_empty(&self) -> bool {
        self.count() == 0
    }

    pub fn clear(&self) {
        unsafe { rocksdb_ffi::rocksdb_writebatch_clear(self.inner) }
    }
}

impl Drop for WriteBatch {
    fn drop(&mut self) {
        unsafe { rocksdb_ffi::rocksdb_writebatch_destroy(self.inner) }
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        unsafe {
            self.cfs.clear();
            rocksdb_ffi::rocksdb_close(self.inner);
        }
    }
}

impl Writable for WriteBatch {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            rocksdb_ffi::rocksdb_writebatch_put(self.inner,
                                                key.as_ptr(),
                                                key.len() as size_t,
                                                value.as_ptr(),
                                                value.len() as size_t);
            Ok(())
        }
    }

    fn put_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            rocksdb_ffi::rocksdb_writebatch_put_cf(self.inner,
                                                   cf.inner,
                                                   key.as_ptr(),
                                                   key.len() as size_t,
                                                   value.as_ptr(),
                                                   value.len() as size_t);
            Ok(())
        }
    }

    fn merge(&self, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            rocksdb_ffi::rocksdb_writebatch_merge(self.inner,
                                                  key.as_ptr(),
                                                  key.len() as size_t,
                                                  value.as_ptr(),
                                                  value.len() as size_t);
            Ok(())
        }
    }

    fn merge_cf(&self, cf: &CFHandle, key: &[u8], value: &[u8]) -> Result<(), String> {
        unsafe {
            rocksdb_ffi::rocksdb_writebatch_merge_cf(self.inner,
                                                     cf.inner,
                                                     key.as_ptr(),
                                                     key.len() as size_t,
                                                     value.as_ptr(),
                                                     value.len() as size_t);
            Ok(())
        }
    }

    fn delete(&self, key: &[u8]) -> Result<(), String> {
        unsafe {
            rocksdb_ffi::rocksdb_writebatch_delete(self.inner, key.as_ptr(), key.len() as size_t);
            Ok(())
        }
    }

    fn delete_cf(&self, cf: &CFHandle, key: &[u8]) -> Result<(), String> {
        unsafe {
            rocksdb_ffi::rocksdb_writebatch_delete_cf(self.inner,
                                                      cf.inner,
                                                      key.as_ptr(),
                                                      key.len() as size_t);
            Ok(())
        }
    }
}

pub struct DBVector {
    base: *mut u8,
    len: usize,
}

impl Deref for DBVector {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.base, self.len) }
    }
}

impl Drop for DBVector {
    fn drop(&mut self) {
        unsafe {
            libc::free(self.base as *mut c_void);
        }
    }
}

impl DBVector {
    pub fn from_c(val: *mut u8, val_len: size_t) -> DBVector {
        DBVector {
            base: val,
            len: val_len as usize,
        }
    }

    pub fn to_utf8(&self) -> Option<&str> {
        from_utf8(self.deref()).ok()
    }
}

#[cfg(test)]
mod test {
    use rocksdb_options::*;
    use std::str;
    use super::*;
    use tempdir::TempDir;

    #[test]
    fn external() {
        let path = TempDir::new("_rust_rocksdb_externaltest").expect("");
        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
        let p = db.put(b"k1", b"v1111");
        assert!(p.is_ok());
        let r: Result<Option<DBVector>, String> = db.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");
        assert!(db.delete(b"k1").is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }

    #[allow(unused_variables)]
    #[test]
    fn errors_do_stuff() {
        let path = TempDir::new("_rust_rocksdb_error").expect("");
        let path_str = path.path().to_str().unwrap();
        let db = DB::open_default(path_str).unwrap();
        let opts = Options::new();
        // The DB will still be open when we try to destroy and the lock should fail
        match DB::destroy(&opts, path_str) {
            Err(ref s) => assert!(s.contains("LOCK: No locks available")),
            Ok(_) => panic!("should fail"),
        }
    }

    #[test]
    fn writebatch_works() {
        let path = TempDir::new("_rust_rocksdb_writebacktest").expect("");
        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();

        // test put
        let batch = WriteBatch::new();
        assert!(db.get(b"k1").unwrap().is_none());
        assert_eq!(batch.count(), 0);
        assert!(batch.is_empty());
        let _ = batch.put(b"k1", b"v1111");
        assert_eq!(batch.count(), 1);
        assert!(!batch.is_empty());
        assert!(db.get(b"k1").unwrap().is_none());
        let p = db.write(batch);
        assert!(p.is_ok());
        let r: Result<Option<DBVector>, String> = db.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");

        // test delete
        let batch = WriteBatch::new();
        let _ = batch.delete(b"k1");
        assert_eq!(batch.count(), 1);
        assert!(!batch.is_empty());
        let p = db.write(batch);
        assert!(p.is_ok());
        assert!(db.get(b"k1").unwrap().is_none());
    }

    #[test]
    fn iterator_test() {
        let path = TempDir::new("_rust_rocksdb_iteratortest").expect("");

        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
        db.put(b"k1", b"v1111").expect("");
        db.put(b"k2", b"v2222").expect("");
        db.put(b"k3", b"v3333").expect("");
        let mut iter = db.iter();
        iter.seek(SeekKey::Start);
        for (k, v) in &mut iter {
            println!("Hello {}: {}",
                     str::from_utf8(&*k).unwrap(),
                     str::from_utf8(&*v).unwrap());
        }
    }

    #[test]
    fn approximate_size_test() {
        let path = TempDir::new("_rust_rocksdb_iteratortest").expect("");
        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
        for i in 1..8000 {
            db.put(format!("{:04}", i).as_bytes(),
                     format!("{:04}", i).as_bytes())
                .expect("");
        }
        db.flush(true).expect("");
        assert!(db.get(b"0001").expect("").is_some());
        db.flush(true).expect("");
        let sizes = db.get_approximate_sizes(&[Range::new(b"0000", b"2000"),
                                               Range::new(b"2000", b"4000"),
                                               Range::new(b"4000", b"6000"),
                                               Range::new(b"6000", b"8000"),
                                               Range::new(b"8000", b"9999")]);
        assert_eq!(sizes.len(), 5);
        for s in &sizes[0..4] {
            assert!(*s > 0);
        }
        assert_eq!(sizes[4], 0);
    }

    #[test]
    fn property_test() {
        let path = TempDir::new("_rust_rocksdb_propertytest").expect("");
        let db = DB::open_default(path.path().to_str().unwrap()).unwrap();
        db.put(b"a1", b"v1").unwrap();
        db.flush(true).unwrap();
        let prop_name = "rocksdb.total-sst-files-size";
        let st1 = db.get_property_int(prop_name).unwrap();
        assert!(st1 > 0);
        db.put(b"a2", b"v2").unwrap();
        db.flush(true).unwrap();
        let st2 = db.get_property_int(prop_name).unwrap();
        assert!(st2 > st1);
    }

    #[test]
    fn list_column_families_test() {
        let path = TempDir::new("_rust_rocksdb_list_column_families_test").expect("");
        let mut cfs = ["default", "cf1", "cf2", "cf3"];
        {
            let mut cfs_opts = vec![];
            for _ in 0..cfs.len() {
                cfs_opts.push(Options::new());
            }
            let cfs_ref_opts: Vec<&Options> = cfs_opts.iter().collect();

            let mut opts = Options::new();
            opts.create_if_missing(true);
            let mut db = DB::open(&opts, path.path().to_str().unwrap()).unwrap();
            for (&cf, &cf_opts) in cfs.iter().zip(&cfs_ref_opts) {
                if cf == "default" {
                    continue;
                }
                db.create_cf(cf, cf_opts).unwrap();
            }
        }
        let opts_list_cfs = Options::new();
        let mut cfs_vec = DB::list_column_families(&opts_list_cfs, path.path().to_str().unwrap())
            .unwrap();
        cfs_vec.sort();
        cfs.sort();
        assert_eq!(cfs_vec, cfs);
    }
}

#[test]
fn snapshot_test() {
    let path = "_rust_rocksdb_snapshottest";
    {
        let db = DB::open_default(path).unwrap();
        let p = db.put(b"k1", b"v1111");
        assert!(p.is_ok());

        let snap = db.snapshot();
        let mut r: Result<Option<DBVector>, String> = snap.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");

        r = db.get(b"k1");
        assert!(r.unwrap().unwrap().to_utf8().unwrap() == "v1111");

        let p = db.put(b"k2", b"v2222");
        assert!(p.is_ok());

        assert!(db.get(b"k2").unwrap().is_some());
        assert!(snap.get(b"k2").unwrap().is_none());
    }
    let opts = Options::new();
    assert!(DB::destroy(&opts, path).is_ok());
}
