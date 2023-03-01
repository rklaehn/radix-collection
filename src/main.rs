use radixdb::{
    store::{Blob, BlobStore, Detached},
    RadixTree,
};
use std::{
    env::args,
    fs, io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

/// Recursively traverse a directory and return all paths
///
/// thanks chatgpt
fn traverse_dir(path: &std::path::Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries {
            if let Ok(entry) = entry {
                let entry_path = entry.path();
                paths.push(entry_path.clone());

                if entry_path.is_dir() {
                    let mut sub_paths = traverse_dir(&entry_path);
                    paths.append(&mut sub_paths);
                }
            }
        }
    }

    paths
}

fn main() -> io::Result<()> {
    let root = args().nth(1).expect("No path given");
    let root = PathBuf::from(root);
    let paths = traverse_dir(root.as_path());
    let mut tree = RadixTree::<Detached>::default();
    let mut list = Vec::<(String, Vec<u8>)>::new();
    for path in paths {
        let relative = path.strip_prefix(&root).unwrap();
        let absolute = path.clone();
        absolute.canonicalize()?;
        let hash = if path.is_file() {
            let data = fs::read(&path)?;
            let hash = blake3::hash(&data);
            Some(hash)
        } else {
            None
        };
        let path = path.to_str().unwrap().to_owned();
        let absolute = absolute.to_str().unwrap().to_owned()    ;
        let relative = relative.to_str().unwrap().to_owned();
        if let Some(hash) = hash {
            println!("Inserting {} {}", relative, hash);
            list.push((relative.clone(), hash.as_bytes().to_vec()));
            tree.insert(relative, absolute);
        }
    }
    let flat = postcard::to_allocvec(&list).unwrap();
    let hash_size = list.len() * 32;
    let store = BytesStore::default();
    // attach the tree to the store. this persists the tree to the store
    let _tree2 = tree.try_attached(store.clone()).unwrap();
    println!("count {}", list.len());
    println!("hash size {}", hash_size);
    println!("flat names: {}", flat.len() - hash_size);
    println!("tree names: {}", store.len() - hash_size as u64);
    for (k, v) in tree.scan_prefix("sound/usb/") {
        println!("{} {:?}", std::str::from_utf8(k.as_ref()).unwrap(), v);
    }
    Ok(())
}

/// A simple in memory store
#[derive(Default, Debug, Clone)]
pub struct BytesStore {
    data: Arc<Mutex<Vec<u8>>>,
}

impl BytesStore {
    pub fn len(&self) -> u64 {
        self.data.lock().unwrap().len() as u64
    }
}

impl BlobStore for BytesStore {
    type Error = anyhow::Error;

    fn read(&self, id: &[u8]) -> std::result::Result<Blob<'static>, Self::Error> {
        let data = self.data.lock().unwrap();
        // decode the id as offset
        let (offset, rest) = unsigned_varint::decode::u64(id).unwrap();
        anyhow::ensure!(rest.is_empty());
        // decode the data at offset as length prefix
        let (len, rest) = unsigned_varint::decode::u64(&data[offset as usize..]).unwrap();
        let len: usize = len.try_into().unwrap();
        anyhow::ensure!(rest.len() >= len);
        Ok(Blob::copy_from_slice(rest[..len].as_ref()))
    }

    fn write(&self, slice: &[u8]) -> std::result::Result<Vec<u8>, Self::Error> {
        let mut data = self.data.lock().unwrap();
        let mut len_buf = unsigned_varint::encode::u64_buffer();
        let mut ofs_buf = unsigned_varint::encode::u64_buffer();
        let len = unsigned_varint::encode::u64(slice.len() as u64, &mut len_buf);
        let ofs = unsigned_varint::encode::u64(data.len() as u64, &mut ofs_buf);
        data.extend_from_slice(len);
        data.extend_from_slice(slice);
        Ok(ofs.to_vec())
    }

    fn sync(&self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}
