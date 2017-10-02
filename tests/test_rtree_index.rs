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
extern crate integer_encoding;
extern crate libc;
extern crate librocksdb_sys as ffi;
extern crate rocksdb;

use std::cmp::Ordering;
use std::{slice, str};
use std::mem;

use self::integer_encoding::VarInt;

use rocksdb::{BlockBasedOptions, BlockBasedIndexType, DB, DBIterator, Options};


/// Return the slice that is prefixed with an unsigned 32-bit varint and the offset after
/// the slice that was read
fn get_length_prefixed_slice(data: &[u8]) -> (&[u8], usize) {
    let (size, slice_start) = u32::decode_var(data);
    let slice_end = slice_start + size as usize;
    let slice = &data[slice_start..slice_end];
    (slice, slice_end)
}

fn values_from_iter(iter: DBIterator) -> Vec<String>{
    let mut vec = Vec::new();
    for (_kk, vv) in iter {
        let value = String::from_utf8((*vv).to_vec()).unwrap();
        vec.push(value);
    }
    vec
}

fn serialize_key(keypath: &str, iid: u64 , aa_min: f64, aa_max: f64, bb_min: f64, bb_max: f64) -> Vec<u8> {
    let mut key = Vec::new();
    key.append(&mut keypath.len().encode_var_vec());
    key.extend_from_slice(keypath.as_bytes());
    // The R-tree stores boxes, hence duplicate the input values
    key.extend_from_slice(&unsafe{ mem::transmute::<u64, [u8; 8]>(iid) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_min) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_max) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_min) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_max) });

    return key;
}

fn serialize_query(keypath: &str, iid_min: u64, iid_max: u64, aa_min: f64, aa_max: f64, bb_min: f64, bb_max: f64)
    -> Vec<u8> {
        let mut key = Vec::new();
        key.append(&mut keypath.len().encode_var_vec());
        key.extend_from_slice(keypath.as_bytes());
        key.extend_from_slice(&unsafe{ mem::transmute::<u64, [u8; 8]>(iid_min) });
        key.extend_from_slice(&unsafe{ mem::transmute::<u64, [u8; 8]>(iid_max) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_min) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_max) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_min) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_max) });
        return key;
}

fn compare_keys_rtree(aa: &[u8], bb: &[u8]) -> Ordering {
    if aa.len() == 0 && bb.len() == 0{
        return Ordering::Equal;
    } else if aa.len() == 0 {
        return Ordering::Less;
    } else if bb.len() == 0 {
        return Ordering::Greater;
    }

    let (keypath_aa, offset_aa) = get_length_prefixed_slice(aa);
    let (keypath_bb, offset_bb) = get_length_prefixed_slice(bb);

    // The ordering of the keypath doesn't need to be unicode collated. The ordering
    // doesn't really matters, it only matters that it's always the same.
    let keypath_compare = keypath_aa.cmp(keypath_bb);
    if keypath_compare != Ordering::Equal {
        return keypath_compare;
    }

    // Keypaths are the same, compare the Internal Ids value
    let seq_aa = unsafe {
        let array = *(aa[(offset_aa)..].as_ptr() as *const [_; 8]);
        mem::transmute::<[u8; 8], u64>(array)
    };
    let seq_bb = unsafe {
        let array = *(bb[(offset_bb)..].as_ptr() as *const [_; 8]);
        mem::transmute::<[u8; 8], u64>(array)
    };
    let seq_compare = seq_aa.cmp(&seq_bb);
    if seq_compare != Ordering::Equal {
        return seq_compare;
    }

    // Internal Ids are the same, compare the bounding box
    let bbox_aa = unsafe {
        let array = *(aa[(offset_aa + 8)..].as_ptr() as *const [_; 32]);
        mem::transmute::<[u8; 32], [f64; 4]>(array)
    };
    let bbox_bb = unsafe {
        let array = *(bb[(offset_bb + 8)..].as_ptr() as *const [_; 32]);
        mem::transmute::<[u8; 32], [f64; 4]>(array)
    };

    for (value_aa, value_bb) in bbox_aa.into_iter().zip(bbox_bb.into_iter()) {
        let value_compare = value_aa.partial_cmp(value_bb).unwrap();
        if value_compare != Ordering::Equal {
            return value_compare;
        }
    }
    // No early return, the values are fully equal
    return Ordering::Equal;
}

#[test]
pub fn test_rtree_index() {
    let path = "_rust_rocksdb_rtreeindextest";

    let mut opts = Options::default();
    opts.create_if_missing(true);

    let mut rtree_opts = Options::default();
    rtree_opts.set_memtable_skip_list_mbb_rep();

    rtree_opts.create_if_missing(true);
    let mut block_based_opts = BlockBasedOptions::default();
    block_based_opts.set_index_type(BlockBasedIndexType::RtreeSearch);
    block_based_opts.set_flush_block_policy_noise();
    rtree_opts.set_block_based_table_factory(&block_based_opts);
    rtree_opts.set_comparator("noise_rtree_cmp", compare_keys_rtree);

    let keypath = "somekeypath";
    let otherkeypath = "anotherkeypath";

    // Start a new scope, else the database can't be destroyed at the end of the test
    {
        let mut db = DB::open(&opts, path).unwrap();
        let rtree = db.create_cf("rtree", &rtree_opts).unwrap();

        let augsburg_key = serialize_key(keypath, 10, 10.75, 11.11, 48.24, 48.50);
        let augsburg_key_slice = unsafe {
            slice::from_raw_parts(augsburg_key.as_ptr() as *const u8, augsburg_key.len())
        };
        db.put_cf(rtree, augsburg_key_slice, b"augsburg").unwrap();
        let alameda_key = serialize_key(keypath, 25, -122.34, -122.22, 37.71, 37.80);
        let alameda_key_slice = unsafe {
            slice::from_raw_parts(alameda_key.as_ptr() as *const u8, alameda_key.len())
        };
        db.put_cf(rtree, alameda_key_slice, b"alameda").unwrap();

        {
            let query = serialize_query(keypath, 2, 15, 10.0, 11.0, 48.0, 49.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["augsburg".to_string()], result);
        }

        {
            let query = serialize_query(keypath, 2, 50, -150.0, 0.0, 20.0, 40.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["alameda".to_string()], result);
        }

        {
            let query = serialize_query(keypath, 2, 3, 10.0, 11.0, 0.0, 0.1);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert!(result.is_empty());
        }

        {
            let query = serialize_query(keypath, 2, 60, -180.0, 180.0, -90.0, 90.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["augsburg".to_string(), "alameda".to_string()],
            result);
        }

        let sydney_key = serialize_key(otherkeypath, 15, 150.26, 151.34, -34.17, -33.36);
        let sydney_key_slice = unsafe {
            slice::from_raw_parts(sydney_key.as_ptr() as *const u8, sydney_key.len())
        };
        db.put_cf(rtree, sydney_key_slice, b"sydney").unwrap();

        {
            let query = serialize_query(keypath, 2, 15, 10.0, 11.0, 48.0, 49.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["augsburg".to_string()], result);
        }

        {
            let query = serialize_query(otherkeypath, 1, 100, -180.0, 180.0, -90.0, 90.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["sydney".to_string()], result);
        }
    }

    // In order to not only test the MemTable, the database needs to be closed (with ending
    // the scope) and re-opened it again. This way the MemTable is flushed into an SSTable.
    {
        let db = DB::open_cf(&opts, path, &[&"rtree"], &[&rtree_opts]).unwrap();

        {
            let query = serialize_query(keypath, 2, 15, 10.0, 11.0, 48.0, 49.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["augsburg".to_string()], result);
        }

        // This one would fail without the right flush block policy
        {
            let query = serialize_query(otherkeypath, 1, 100, -180.0, 180.0, -90.0, 90.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["sydney".to_string()], result);
        }
    }

    assert!(DB::destroy(&opts, path).is_ok());
}
