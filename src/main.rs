//! main.rs — demo for our minimal Page
mod storage;
use storage::page::Page;


fn main() {
    // create & init a page (no special space)
    let mut page = Page::new();
    page.init(0);

    // add two items
    let s1 = page.add_item(b"hello").unwrap();
    let s2 = page.add_item(b"world!").unwrap();

    // read them back
    println!("slot {} = {:?}", s1, page.get_item(s1));
    println!("slot {} = {:?}", s2, page.get_item(s2));
    println!("free space left = {}", page.free_space());
}