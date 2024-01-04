use bytes::BufMut;

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    data: Vec<u8>,
    offsets: Vec<u16>,
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            data: Vec::new(),
            offsets: Vec::new(),
            block_size: block_size,
        }
    }

    // offset + data + num_of_element
    fn estimated_size(&self) -> usize {
        self.offsets.len() * SIZEOF_U16 + self.data.len() + SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        let key_len = key.len();
        let value_len = value.len();
        // estimated_size + key + value + [key.len(2B) + value.len(2B) + offset(2B)]
        if self.estimated_size() + key_len + value_len + SIZEOF_U16 * 3 > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key_len as u16);
        self.data.put(key);
        self.data.put_u16(value_len as u16);
        self.data.put(value);

        return true;
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        return self.data.is_empty();
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
