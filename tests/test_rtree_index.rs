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

use std::{slice, str};
use std::mem;

use self::integer_encoding::VarInt;

use rocksdb::{BlockBasedOptions, BlockBasedIndexType, DB, DBIterator, Options};


fn values_from_iter(iter: DBIterator) -> Vec<String>{
    let mut vec = Vec::new();
    for (_kk, vv) in iter {
        let value = String::from_utf8((*vv).to_vec()).unwrap();
        vec.push(value);
    }
    vec
}

fn serialize_key(keypath: &str, iid: f64 , aa_min: f64, aa_max: f64, bb_min: f64, bb_max: f64) -> Vec<u8> {
    let mut key = Vec::new();
    key.append(&mut keypath.len().encode_var_vec());
    key.extend_from_slice(keypath.as_bytes());
    // The R-tree stores boxes, hence duplicate the input values
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(iid) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(iid) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_min) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_max) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_min) });
    key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_max) });

    return key;
}

fn serialize_query(keypath: &str, iid_min: f64, iid_max: f64, aa_min: f64, aa_max: f64, bb_min: f64, bb_max: f64)
    -> Vec<u8> {
        let mut key = Vec::new();
        key.append(&mut keypath.len().encode_var_vec());
        key.extend_from_slice(keypath.as_bytes());
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(iid_min) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(iid_max) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_min) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(aa_max) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_min) });
        key.extend_from_slice(&unsafe{ mem::transmute::<f64, [u8; 8]>(bb_max) });
        return key;
}

/*
fn deserialize_key(key: Vec<u8>) -> (String, Vec<f64>) {
    let mut key_slice = key.as_slice();

    // Get keypath
    let (size, num_bytes) = u32::decode_var(key_slice);
    let size = size as usize;
    key_slice = &key_slice[num_bytes..];
    let keypath = str::from_utf8(&key_slice[0..size]).unwrap().to_string();
    key_slice = &key_slice[size..];

    // Get other dimensions
    let mut mbb = Vec::new();
    while key_slice.len() > 0 {
        let mut tmp: [u8; 8] = [0; 8];
        tmp.copy_from_slice(&key_slice[0..8]);
        let double = unsafe { mem::transmute::<[u8; 8], f64>(tmp) };
        mbb.push(double);
        key_slice = &key_slice[8..];
    }

    (keypath, mbb)
}
*/


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
    rtree_opts.set_block_based_table_factory(&block_based_opts);

    // Start a new scope, else the database can't be destroyed at the end of the test
    {
        let mut db = DB::open(&opts, path).unwrap();
        let rtree = db.create_cf("rtree", &rtree_opts).unwrap();

        let keypath = "somekeypath";
        let augsburg_key = serialize_key(keypath, 10f64, 10.75, 11.11, 48.24, 48.50);
        let augsburg_key_slice = unsafe {
            slice::from_raw_parts(augsburg_key.as_ptr() as *const u8, augsburg_key.len())
        };
        db.put_cf(rtree, augsburg_key_slice, b"augsburg").unwrap();
        let alameda_key = serialize_key(keypath, 25f64, -122.34, -122.22, 37.71, 37.80);
        let alameda_key_slice = unsafe {
            slice::from_raw_parts(alameda_key.as_ptr() as *const u8, alameda_key.len())
        };
        db.put_cf(rtree, alameda_key_slice, b"alameda").unwrap();

        {
            let query = serialize_query(keypath, 2f64, 15f64, 10.0, 11.0, 48.0, 49.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["augsburg".to_string()], result);
        }

        {
            let query = serialize_query(keypath, 2f64, 50f64, -150.0, 0.0, 20.0, 40.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["alameda".to_string()], result);
        }

        {
            let query = serialize_query(keypath, 2f64, 3f64, 10.0, 11.0, 0.0, 0.1);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert!(result.is_empty());
        }

        {
            let query = serialize_query(keypath, 2f64, 60f64, -180.0, 180.0, -90.0, 90.0);
            let iter = db.rtree_iterator(&query.as_slice());
            let result = values_from_iter(iter);
            assert_eq!(vec!["augsburg".to_string(), "alameda".to_string()],
            result);
        }
    }
    assert!(DB::destroy(&opts, path).is_ok());
}
