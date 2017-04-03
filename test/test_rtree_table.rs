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

use std::{mem, str};

use self::integer_encoding::VarInt;

use rocksdb::{DB, DBIterator, Options, RtreeKey};

// NOTE vmx 2017-03-30: This enum is a copy of the C++ one. Probably the C++ one should be used
// directly so that it is always in sync. But to get things working, it's good enough for now.
#[allow(dead_code)]
#[derive(Debug)]
#[repr(u8)]
enum RtreeDimensionType {
    Null = 0x0,
    Bool = 0x1,
    // Currently there's no differentiation between integers and doubles, but
    // it's reserved for future use
    //Int = 0x2,
    Double = 0x3,
    String = 0x4,
}

#[derive(Debug, PartialEq)]
enum RtreeVariant {
    Double(f64),
    String(String),
}

fn iter_to_vec(iter: DBIterator) -> Vec<(Vec<RtreeVariant>, String)> {
    let mut vec = Vec::new();
    for (kk, vv) in iter {
        let key =  kk.into_vec();
        let mut keys = Vec::new();
        let mut key_slice = key.as_slice();
        while key_slice.len() > 0 {
            let dimension_type: RtreeDimensionType = unsafe { mem::transmute(key_slice[0]) };
            key_slice = &key_slice[1..];
            let val_double: f64;
            let val_string: String;
            match dimension_type {
                RtreeDimensionType::Double => {
                    let mut tmp: [u8; 8] = [0; 8];
                    tmp.copy_from_slice(&key_slice[0..8]);
                    val_double = unsafe { mem::transmute::<[u8; 8], f64>(tmp) };
                    keys.push(RtreeVariant::Double(val_double));
                    key_slice = &key_slice[8..];
                }
                RtreeDimensionType::String => {
                    let (size, num_bytes) = u32::decode_var(key_slice);
                    let size = size as usize;
                    key_slice = &key_slice[num_bytes..];
                    val_string = str::from_utf8(&key_slice[0..size]).unwrap().to_string();
                    keys.push(RtreeVariant::String(val_string));
                    key_slice = &key_slice[size..];
                }
                _ => {
                    panic!("this type ({:?}) not yet implemented", dimension_type);
                }
            }
        }
        let value = String::from_utf8((*vv).to_vec()).unwrap();
        vec.push((keys, value));
    }
    vec
}

#[test]
pub fn test_rtree_table_doubles() {
    let path = "_rust_rocksdb_rtreetabletest_doubles";

    let mut opts = Options::default();
    opts.set_rtree_table();
    opts.create_if_missing(true);

    // Start a new scope, else the database can't be destroyed at the end of the test
    {
        let db = DB::open(&opts, path).unwrap();

        let mut augsburg_key = RtreeKey::default();
        augsburg_key.push_double(10.75);
        augsburg_key.push_double(11.11);
        augsburg_key.push_double(48.24);
        augsburg_key.push_double(48.50);
        let augsburg_key_slice = augsburg_key.as_slice();
        db.put(augsburg_key_slice, b"augsburg").unwrap();

        let mut alameda_key = RtreeKey::default();
        alameda_key.push_double(-122.34);
        alameda_key.push_double(-122.22);
        alameda_key.push_double( 37.7);
        alameda_key.push_double(37.80);
        let alameda_key_slice = alameda_key.as_slice();
        db.put(alameda_key_slice, b"alameda").unwrap();

        {
            let mut query = RtreeKey::default();
            query.push_double(10.0);
            query.push_double(11.0);
            query.push_double(48.0);
            query.push_double(49.0);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert_eq!(vec![
                (vec![
                    RtreeVariant::Double(10.75),
                    RtreeVariant::Double(11.11),
                    RtreeVariant::Double(48.24),
                    RtreeVariant::Double(48.50)],
                 "augsburg".to_string())], result);
        }
        {
            let mut query = RtreeKey::default();
            query.push_double(-150.0);
            query.push_double(0.0);
            query.push_double(20.0);
            query.push_double(40.0);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert_eq!(vec![
                (vec![
                    RtreeVariant::Double(-122.34),
                    RtreeVariant::Double(-122.22),
                    RtreeVariant::Double( 37.7),
                    RtreeVariant::Double(37.80)],
                 "alameda".to_string())], result);
        }
        {
            let mut query = RtreeKey::default();
            query.push_double(10.0);
            query.push_double(11.0);
            query.push_double(0.0);
            query.push_double(0.1);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert!(result.is_empty());
        }
        {
            let mut query = RtreeKey::default();
            query.push_double(-180.0);
            query.push_double(180.0);
            query.push_double(-90.0);
            query.push_double(90.0);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert_eq!(vec![
                (vec![
                    RtreeVariant::Double(-122.34),
                    RtreeVariant::Double(-122.22),
                    RtreeVariant::Double( 37.7),
                    RtreeVariant::Double(37.80)],
                 "alameda".to_string()),
                (vec![
                    RtreeVariant::Double(10.75),
                    RtreeVariant::Double(11.11),
                    RtreeVariant::Double(48.24),
                    RtreeVariant::Double(48.50)],
                 "augsburg".to_string())], result);
        }
    }
    assert!(DB::destroy(&opts, path).is_ok());
}


#[test]
pub fn test_rtree_table_strings() {
    let path = "_rust_rocksdb_rtreetabletest_strings";

    let mut opts = Options::default();
    opts.set_rtree_table();
    opts.create_if_missing(true);

    // Start a new scope, else the database can't be destroyed at the end of the test
    {
        let db = DB::open(&opts, path).unwrap();

        let mut noise_key = RtreeKey::default();
        noise_key.push_string("somestring");
        noise_key.push_string("somestring");
        noise_key.push_double(33.0);
        noise_key.push_double(33.0);
        let noise_key_slice = noise_key.as_slice();
        db.put(noise_key_slice, b"the actual value might be here").unwrap();

        let mut noise_another_key = RtreeKey::default();
        noise_another_key.push_string("yetanotherkey");
        noise_another_key.push_string("yetanotherkey");
        noise_another_key.push_double(58.0);
        noise_another_key.push_double(58.0);
        let noise_another_key_slice = noise_another_key.as_slice();
        db.put(noise_another_key_slice, b"yet another value").unwrap();

        {
            let mut query = RtreeKey::default();
            query.push_string("somestring");
            query.push_string("somestring");
            query.push_double(0.0);
            query.push_double(100.0);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert_eq!(vec![
                (vec![
                    RtreeVariant::String("somestring".to_string()),
                    RtreeVariant::String("somestring".to_string()),
                    RtreeVariant::Double(33.0),
                    RtreeVariant::Double(33.0)],
                 "the actual value might be here".to_string())], result);
        }
        {
            let mut query = RtreeKey::default();
            query.push_string("yetanotherkey");
            query.push_string("yetanotherkey");
            query.push_double(0.0);
            query.push_double(100.0);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert_eq!(vec![
                (vec![
                    RtreeVariant::String("yetanotherkey".to_string()),
                    RtreeVariant::String("yetanotherkey".to_string()),
                    RtreeVariant::Double(58.0),
                    RtreeVariant::Double(58.0)],
                 "yet another value".to_string())], result);
        }
        {
            let mut query = RtreeKey::default();
            query.push_string("a");
            query.push_string("z");
            query.push_double(40.0);
            query.push_double(70.0);
            let iter = db.rtree_iterator(&query);
            let result = iter_to_vec(iter);
            assert_eq!(vec![
                (vec![
                    RtreeVariant::String("yetanotherkey".to_string()),
                    RtreeVariant::String("yetanotherkey".to_string()),
                    RtreeVariant::Double(58.0),
                    RtreeVariant::Double(58.0)],
                 "yet another value".to_string())], result);
        }
    }
    assert!(DB::destroy(&opts, path).is_ok());
}
