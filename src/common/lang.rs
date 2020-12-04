/// These are very low-level utilities where I consider
/// it a shortcoming of the language that there isn't something
/// I can already use.

pub fn rvec(i: i32, j: i32) -> Vec<i32> {
    (i..j).collect()
}
