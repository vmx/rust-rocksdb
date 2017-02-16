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

use std::{slice, str};
use std::mem::size_of;

use rocksdb::{DB, DBIterator, Direction, IteratorMode, Options};

fn iter_to_vec(iter: DBIterator) -> Vec<(Vec<f64>, String)>{
    let mut vec = Vec::new();
    for (kk, vv) in iter {
        let key =  unsafe {
            Vec::from_raw_parts(kk.as_ptr() as *mut f64,
                                kk.len() / size_of::<f64>(),
                                kk.len() / size_of::<f64>())
        };
        let foo = key.clone();
        let value = String::from_utf8((*vv).to_vec()).unwrap();
        vec.push((foo, value));
    }
    vec
}

#[test]
pub fn test_rtree_table() {
    let path = "_rust_rocksdb_rtreetabletest";

    let mut opts = Options::default();
    opts.set_rtree_table(2);
    opts.create_if_missing(true);

    // Start a new scope, else the database can't be destroyed at the end of the test
    {
        let db = DB::open(&opts, path).unwrap();

        let augsburg_key: Vec<f64> = vec![10.75, 11.11, 48.24, 48.50];
        let augsburg_key_slice = unsafe {
            slice::from_raw_parts(augsburg_key.as_ptr() as *const u8,
                                  augsburg_key.len() * size_of::<f64>())
        };
        db.put(augsburg_key_slice, b"augsburg").unwrap();
        let alameda_key: Vec<f64> = vec![-122.34, -122.22, 37.71, 37.80];
        let alameda_key_slice = unsafe {
            slice::from_raw_parts(alameda_key.as_ptr() as *const u8,
                                  alameda_key.len() * size_of::<f64>())
        };
        db.put(alameda_key_slice, b"alameda").unwrap();

        {
            let query: Vec<f64> = vec![10.0, 11.0, 48.0, 49.0];
            //let query: Vec<f64> = vec![-150.0, 0.0, 20.0, 40.0];
            let query_slice = unsafe {
                slice::from_raw_parts(query.as_ptr() as *const u8,
                                      query.len() * size_of::<f64>())
            };
            let iter = db.iterator(IteratorMode::From(query_slice, Direction::Forward));
            let result = iter_to_vec(iter);
            assert_eq!(vec![(augsburg_key.clone(), "augsburg".to_string())], result);
        }

        {
            let query: Vec<f64> = vec![-150.0, 0.0, 20.0, 40.0];
            let query_slice = unsafe {
                slice::from_raw_parts(query.as_ptr() as *const u8,
                                      query.len() * size_of::<f64>())
            };
            let iter = db.iterator(IteratorMode::From(query_slice, Direction::Forward));
            let result = iter_to_vec(iter);
            assert_eq!(vec![(alameda_key.clone(), "alameda".to_string())], result);
        }

        {
            let query: Vec<f64> = vec![10.0, 11.0, 0.0, 0.1];
            let query_slice = unsafe {
                slice::from_raw_parts(query.as_ptr() as *const u8,
                                      query.len() * size_of::<f64>())
            };
            let iter = db.iterator(IteratorMode::From(query_slice, Direction::Forward));
            let result = iter_to_vec(iter);
            assert!(result.is_empty());
        }

        {
            let query: Vec<f64> = vec![-180.0, 180.0, -90.0, 90.0];
            let query_slice = unsafe {
                slice::from_raw_parts(query.as_ptr() as *const u8,
                                      query.len() * size_of::<f64>())
            };
            let iter = db.iterator(IteratorMode::From(query_slice, Direction::Forward));
            let result = iter_to_vec(iter);
            assert_eq!(vec![(alameda_key.clone(), "alameda".to_string()),
                            (augsburg_key.clone(), "augsburg".to_string())],
                       result);
        }
    }
    assert!(DB::destroy(&opts, path).is_ok());
}
