mod log;

fn main() {
    println!("Hello, world! 123");
    let p1 = Point::new(10, 20);
    let p2 = p1; // Copy occurs here
    println!("the p1 is {:?}", p1);
    println!("the p2 is {:?}", p2);

    let mut s = String::from("hello world");
    let r2 = &s;
    println!("the r1 is {:?}", r2);

    let r1 = &mut s;
    r1.push_str(" testing");

    let bytes = s.as_bytes();

    for (i, &item) in bytes.iter().enumerate() {
        if item == b' ' {
            break;
        }
    }
}

#[derive(Debug)]
struct Point {
    x: i32,
    y: i32,
}

impl Point {
    fn new(x: i32, y: i32) -> Point {
        Point { x, y }
    }
}

impl Clone for Point {
    fn clone(&self) -> Self {
        Point {
            x: self.x,
            y: self.y,
        }
    }
}

impl Copy for Point {}
