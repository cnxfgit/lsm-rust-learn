use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0,
            BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
        ))
    }

    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&table)?;
        let iter = Self {
            block_iter,
            table,
            block_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&self.table)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        let mut block_idx = table.find_block_idx(key);
        let mut block_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(block_idx)?, key);
        if !block_iter.is_valid() {
            block_idx += 1;
            if block_idx < table.num_of_blocks() {
                block_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(block_idx)?);
            }
        }
        Ok((block_idx, block_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&table,key)?;
        let iter = Self {
            block_iter,
            table,
            block_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                self.block_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.block_idx)?,
                );
            }
        }
        Ok(())
    }
}
