(: case-insensitive, dot-all, and extended-mode at the same time :)
fn:replace("
HELLO
world
", "h ello.*world", "hoi", "isx")
