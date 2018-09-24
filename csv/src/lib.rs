extern crate csv;
extern crate libc;

use std::ffi::CStr;
use libc::{c_char, c_int, size_t};
use std::ptr;
use csv::{Reader as CsvReader, ReaderBuilder, ByteRecordsIter, ByteRecord, Error as CsvError,};

pub struct Field (Vec<u8>);

#[no_mangle]
pub unsafe extern "C" fn field_ptr(fp: *const Field) -> *const c_char {
    let f = match fp.as_ref() {
        Some(x) => x,
        None => return ptr::null(),
    };
    f.0.as_ptr() as *const c_char
}

#[no_mangle]
pub unsafe extern "C" fn field_len(fp: *const Field) -> size_t {
    let f = match fp.as_ref() {
        Some(x) => x,
        None => return 0,
    };
    f.0.len() as size_t
}

pub struct Record {
    host: Field,
    start_ts: Field,
    end_ts: Field,
}
#[no_mangle]
pub extern "C" fn record_new() -> *mut Record {
    let r = Record {
        host: Field(Vec::new()),
        start_ts: Field(Vec::new()),
        end_ts: Field(Vec::new()),
    };
    Box::into_raw(Box::new(r))
}

#[no_mangle]
pub unsafe extern "C" fn record_host(rp: *const Record) -> *const Field {
    let r = match rp.as_ref() {
        Some(x) => x,
        None => return ptr::null(),
    };
    &r.host as *const Field
}
#[no_mangle]
pub unsafe  extern "C" fn record_start_ts(rp: *const Record) -> *const Field {
    let r = match rp.as_ref() {
        Some(x) => x,
        None => return ptr::null(),
    };
    &r.start_ts as *const Field
}
#[no_mangle]
pub unsafe extern "C" fn record_end_ts(rp: *const Record) -> *const Field {
    let r = match rp.as_ref() {
        Some(x) => x,
        None => return ptr::null(),
    };
    &r.end_ts as *const Field
}

pub enum Reader {
    File(CsvReader<std::fs::File>),
    Stdin(CsvReader<std::io::Stdin>),
}

pub enum RecordsIter {
    File(ByteRecordsIter<'static, std::fs::File>),
    Stdin(ByteRecordsIter<'static, std::io::Stdin>),
}

#[no_mangle]
pub unsafe extern "C" fn reader_from_path(path: *const c_char) -> *mut Reader {
    let p = match path.as_ref() {
        Some(cs_p) => match CStr::from_ptr(cs_p).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
        None => return ptr::null_mut(),
    };

    match ReaderBuilder::new()
        .has_headers(true)
        .from_path(p) 
    {
        Ok(r) => {
            return Box::into_raw(Box::new(Reader::File(r)));
        }
        Err(_) => return ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn reader_from_stdin() -> *mut Reader {
    let r = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(std::io::stdin());

    return Box::into_raw(Box::new(Reader::Stdin(r)));
}

#[no_mangle]
pub unsafe extern "C" fn reader_free(rdr_p: *mut Reader) {
    if !rdr_p.is_null() {
        Box::from_raw(rdr_p);
    }
}

#[no_mangle]
pub unsafe extern "C" fn reader_iter(rdr_p: *mut Reader) -> *mut RecordsIter {
    let rdr = match rdr_p.as_mut() {
        Some(x) => x,
        None => return ptr::null_mut(),
    };
    match rdr {
        Reader::File(r) =>
            Box::into_raw(Box::new(RecordsIter::File(r.byte_records()))),
        Reader::Stdin(r) =>
            Box::into_raw(Box::new(RecordsIter::Stdin(r.byte_records()))),
    }
}

#[inline]
fn fill_rec(inr: Result<ByteRecord, CsvError>, outr: &mut Record) -> Result<(), i8> {
    match inr {
        Ok(brec) => {
            if brec.len() < 3 || brec.len() > 3 {
                return Err(-1);
            }
            outr.host.0.clear();
            outr.start_ts.0.clear();
            outr.end_ts.0.clear();

            outr.host.0.extend_from_slice(&brec[0]);
            outr.start_ts.0.extend_from_slice(&brec[1]);
            outr.end_ts.0.extend_from_slice(&brec[2]);

            return Ok(());
        }
        Err(_) => return Err(-2),
    }
}
    
#[no_mangle]
pub unsafe extern "C" fn read_next(
    rec_iter_p: *mut RecordsIter,
    rec_p: *mut Record,
) -> c_int {
    let outr = match rec_p.as_mut() {
        Some(x) => x,
        None => return -1,
    };
    match rec_iter_p.as_mut() {
        Some(x) => match x {
            RecordsIter::File(it) => {
                if let Some(inr) = it.next() {
                    if let Err(_) = fill_rec(inr, outr) {
                        return -3
                    }
                } else {
                    return 1;
                }
            }
            RecordsIter::Stdin(it) => {
                if let Some(inr) = it.next() {
                    if let Err(_) = fill_rec(inr, outr) {
                        return -3
                    }
                } else {
                    return 1;
                }
            }
        }
        None => return -2,
    };
    
    return 0;
}
    
#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use super::*;
    
    #[test]
    fn test_csv_from_file() {
        unsafe {
            let s = CString::new("params.csv").unwrap();
            let r = reader_from_path(s.as_ptr());
            assert!(!r.is_null());
            let it = reader_iter(r);
            assert!(!it.is_null());

            let recp: *mut Record = record_new();
            let mut err: c_int;
            loop {
                err = read_next(it, recp);
                if err < 0 {
                    panic!("read_next failed with '{}'", err);
                } else if err > 0 {
                    break;
                }
                let host = record_host(recp);
                assert!(!field_ptr(host).is_null()); 
                assert!(field_len(host) == 11);

                let start_ts = record_start_ts(recp);
                assert!(!field_ptr(start_ts).is_null()); 
                assert!(field_len(start_ts) == 19);

                let end_ts = record_end_ts(recp);
                assert!(!field_ptr(end_ts).is_null()); 
                assert!(field_len(end_ts) == 19);
            }
        }
    }
}

            
            


        
            

    


    
