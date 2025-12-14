pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_add_two_numbers() {
        // given
        let left = 2;
        let right = 2;

        // when
        let result = add(left, right);

        // then
        assert_eq!(result, 4);
    }
}
