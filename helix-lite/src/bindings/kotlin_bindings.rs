use jni::sys::{jlong, jstring};
// kotlin_binding.rs
use jni::objects::{JClass, JString};
use jni::JNIEnv;

use crate::HelixEmbedded;

#[no_mangle]
pub extern "system" fn Java_com_helix_HelixKotlin_new(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path: String = env.get_string(&path).unwrap().into();
    let db = Box::new(HelixEmbedded::new(path).unwrap());
    Box::into_raw(db) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_helix_HelixKotlin_query(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    query_id: JString,
    json_body: JString,
) -> jstring {
    let db = unsafe { &*(ptr as *const HelixEmbedded) };
    let query_id: String = env.get_string(&query_id).unwrap().into();
    let json_body: String = env.get_string(&json_body).unwrap().into();

    match db.query(query_id, json_body) {
        Ok(result) => env.new_string(result).unwrap().cast(),
        Err(e) => env.new_string(e.to_string()).unwrap().cast(),
    }
}
