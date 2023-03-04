// Taken from Rust's VecDeque Iter

use super::*;

pub struct Iter<'a, const CAP: usize, A: Aggregator> {
    ring: &'a [Option<A::PartialAggregate>; CAP],
    tail: u8,
    head: u8,
}

impl<'a, const CAP: usize, A: Aggregator> Iter<'a, CAP, A> {
    pub(super) fn new(ring: &'a [Option<A::PartialAggregate>; CAP], tail: u8, head: u8) -> Self {
        Iter { ring, tail, head }
    }
}
impl<'a, const CAP: usize, A: Aggregator> Iterator for Iter<'a, CAP, A> {
    type Item = &'a Option<A::PartialAggregate>;

    #[inline]
    fn next(&mut self) -> Option<&'a Option<A::PartialAggregate>> {
        if self.tail == self.head {
            return None;
        }
        let tail = self.tail;
        self.tail = wrap_index(self.tail.wrapping_add(1), self.ring.len() as u8);
        // Safety:
        // - `self.tail` in a ring buffer is always a valid index.
        // - `self.head` and `self.tail` equality is checked above.
        Some(&self.ring[tail as usize])
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = count(self.tail, self.head, self.ring.len() as u8);
        (len as usize, Some(len as usize))
    }
}

#[cfg(feature = "drill_down")]
pub struct DrillIter<'a, const CAP: usize, A: Aggregator> {
    ring: &'a [Option<Vec<A::PartialAggregate>>; CAP],
    tail: u8,
    head: u8,
}

#[cfg(feature = "drill_down")]
impl<'a, const CAP: usize, A: Aggregator> DrillIter<'a, CAP, A> {
    pub(super) fn new(
        ring: &'a [Option<Vec<A::PartialAggregate>>; CAP],
        tail: u8,
        head: u8,
    ) -> Self {
        DrillIter { ring, tail, head }
    }
}
#[cfg(feature = "drill_down")]
impl<'a, const CAP: usize, A: Aggregator> Iterator for DrillIter<'a, CAP, A> {
    type Item = Option<&'a [A::PartialAggregate]>;
    //type Item = &'a Option<A::PartialAggregate>;

    #[inline]
    fn next(&mut self) -> Option<Option<&'a [A::PartialAggregate]>> {
        if self.tail == self.head {
            return None;
        }
        let tail = self.tail;
        self.tail = wrap_index(self.tail.wrapping_add(1), self.ring.len() as u8);
        // Safety:
        // - `self.tail` in a ring buffer is always a valid index.
        // - `self.head` and `self.tail` equality is checked above.
        Some(self.ring[tail as usize].as_deref())
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = count(self.tail, self.head, self.ring.len() as u8);
        (len as usize, Some(len as usize))
    }
}
