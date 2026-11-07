//! Preserve original test assertions while exercising the production typed read.
impl super::Assembler {
    pub(in crate::connection) fn read(&mut self, max: usize, ordered: bool) -> Option<super::Chunk> {
        self.read_checked(max, ordered).expect("test requires qualified receive inventory")
    }
    pub(in crate::connection) fn clear(&mut self) {
        self.clear_checked().expect("test requires qualified clear inventory");
    }
    pub(in crate::connection) fn reinit(&mut self) {
        self.reinit_checked().expect("test requires qualified reset inventory");
    }
}
