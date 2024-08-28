#![allow(unused_variables)]
mod orderbook;
use std::cmp::Reverse;
use std::collections::VecDeque;
use std::process::Output;
use std::{cell::RefCell, io};

fn test1() {
    let a = [1, 2, 3, 4, 5];

    println!("Please enter an array index.");

    let mut index = String::new();

    io::stdin()
        .read_line(&mut index)
        .expect("Failed to read line");

    let index: usize = index
        .trim()
        .parse()
        .expect("Index entered was not a number");

    let element = a[index];

    println!("The value of the element at index {index} is: {element}");
}

fn loop_test() {
    let mut count = 0;
    'counting_up: loop {
        println!("count = {count}");
        let mut remaining = 10;

        loop {
            println!("remaining = {remaining}");
            if remaining == 9 {
                break;
            }
            if count == 2 {
                break 'counting_up;
            }
            remaining -= 1;
        }

        count += 1;
    }
    println!("End count = {count}");

    for number in (1..4).rev() {
        println!("{number}!");
    }
    println!("LIFTOFF!!!");
}

#[derive(Debug)]
struct Rectangle {
    width: u32,
    height: u32,
}

impl Rectangle {
    fn area(&self) -> u32 {
        return self.width * self.height;
    }
}

fn struct_test() {
    let rect1: Rectangle = Rectangle {
        width: 30,
        height: 50,
    };
    println!(
        "The area of the rectangle is {} square pixels.",
        rect1.area()
    );
}

#[derive(Debug, PartialEq, Copy, Clone)]
enum ShirtColor {
    Red,
    Blue,
}

struct Inventory {
    shirts: Vec<ShirtColor>,
}

impl Inventory {
    fn giveaway(&self, user_preference: Option<ShirtColor>) -> ShirtColor {
        user_preference.unwrap_or_else(|| self.most_stocked())
    }

    fn most_stocked(&self) -> ShirtColor {
        let mut num_red = 0;
        let mut num_blue = 0;

        for color in &self.shirts {
            match color {
                ShirtColor::Red => num_red += 1,
                ShirtColor::Blue => num_blue += 1,
            }
        }
        if num_red > num_blue {
            ShirtColor::Red
        } else {
            ShirtColor::Blue
        }
    }
}

fn closure_test() {
    let store = Inventory {
        shirts: vec![ShirtColor::Blue, ShirtColor::Red, ShirtColor::Blue],
    };

    let user_pref1 = Some(ShirtColor::Red);
    let giveaway1 = store.giveaway(user_pref1);
    println!(
        "The user with preference {:?} gets {:?}",
        user_pref1, giveaway1
    );

    let user_pref2 = None;
    let giveaway2 = store.giveaway(user_pref2);
    println!(
        "The user with preference {:?} gets {:?}",
        user_pref2, giveaway2
    );
    let max = 100;
    let expensive_closure = |num: u32| -> u32 { num + max };

    print!("{}\n", expensive_closure(100));

    let list = vec![1, 2, 3];
    println!("Before defining closure: {list:?}");

    let only_borrows = || println!("From closure: {list:?}");

    println!("Before calling closure: {list:?}");
    only_borrows();
    println!("After calling closure: {list:?}");

    let mut list = vec![1, 2, 3];
    println!("Before defining closure: {list:?}");

    let mut borrows_mutably = || list.push(7);

    borrows_mutably();
    println!("After calling closure: {list:?}");

    let list = vec![1, 2, 3];
    println!("Before defining closure: {list:?}");

    use std::thread;
    thread::spawn(move || println!("From thread: {list:?}"))
        .join()
        .unwrap();
}

fn iterator_test() {
    let v1: Vec<i32> = vec![1, 2, 3];

    let v2: Vec<_> = v1.iter().map(|x| x + 1).collect();

    print!("{v2:?}\n");
}

struct MyBox(i32, i32);

struct CustomSmartPointer {
    data: String,
}

impl Drop for CustomSmartPointer {
    fn drop(&mut self) {
        println!("Dropping CustomSmartPointer with data `{}`!", self.data);
    }
}

enum List {
    Cons(i32, Rc<List>),
    Nil,
}

use orderbook::order::Order;
use orderbook::order::OrderRef;
use orderbook::types::OrderSourceType;
use orderbook::types::{OrdType, Side};
use orderbook::L3Order;
use orderbook::L3OrderRef;
use rayon::range;
use serde::de::{self, value};

use crate::List::{Cons, Nil};
use std::rc::Rc;

fn smart_pointer_test() {
    let x = 5;
    let y = &x;
    print!("{}\n", y);
    assert_eq!(5, x);
    assert_eq!(5, *y);
    let my_box: MyBox = MyBox(1, 2);
    print!("{}\n", my_box.0);
    let c = CustomSmartPointer {
        data: String::from("my stuff"),
    };
    let d = CustomSmartPointer {
        data: String::from("other stuff"),
    };
    println!("CustomSmartPointers created.");

    let a = Rc::new(Cons(5, Rc::new(Cons(10, Rc::new(Nil)))));
    println!("count after creating a = {}", Rc::strong_count(&a));
    let b = Cons(3, Rc::clone(&a));
    println!("count after creating b = {}", Rc::strong_count(&a));
    {
        let c = Cons(4, Rc::clone(&a));
        println!("count after creating c = {}", Rc::strong_count(&a));
    }
    println!("count after c goes out of scope = {}", Rc::strong_count(&a));
}

pub mod test2 {

    #[derive(Debug)]
    pub enum List {
        Cons(Rc<RefCell<i32>>, Rc<List>),
        Nil,
    }

    use crate::List::{Cons, Nil};
    use std::cell::RefCell;
    use std::rc::Rc;
}

fn refcell_test() {
    let value = Rc::new(RefCell::new(5));

    let a = Rc::new(test2::List::Cons(
        Rc::clone(&value),
        Rc::new(test2::List::Nil),
    ));

    let b = test2::List::Cons(Rc::new(RefCell::new(3)), Rc::clone(&a));
    let c = test2::List::Cons(Rc::new(RefCell::new(4)), Rc::clone(&a));

    *value.borrow_mut() += 10;

    println!("a after = {a:?}");
    println!("b after = {b:?}");
    println!("c after = {c:?}");
}

use std::sync::{mpsc, WaitTimeoutResult};
use std::sync::{Arc, Mutex};
use std::thread;

fn thread_test() {
    let v = vec![1, 2, 3];

    let handle = thread::spawn(move || {
        println!("Here's a vector: {v:?}");
    });

    handle.join().unwrap();

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let val = String::from("hi");
        tx.send(val).unwrap();
    });

    let received = rx.recv().unwrap();
    println!("Got: {received}");

    let counter = Arc::new(Mutex::new(0));
    let mut handles = vec![];

    for _ in 0..10 {
        let counter = Arc::clone(&counter);
        let handle = thread::spawn(move || {
            let mut num = counter.lock().unwrap();

            *num += 1;
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    println!("Result: {}", *counter.lock().unwrap());
}

fn web_test() {
    use hello_cargo::ThreadPool;
    use std::{
        fs,
        io::{prelude::*, BufReader},
        net::{TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    fn handle_connection(mut stream: TcpStream) {
        let buf_reader = BufReader::new(&mut stream);
        let request_line = buf_reader.lines().next().unwrap().unwrap();

        let (status_line, filename) = if request_line == "GET / HTTP/1.1" {
            ("HTTP/1.1 200 OK", "hello.html")
        } else {
            ("HTTP/1.1 404 NOT FOUND", "404.html")
        };

        let contents = fs::read_to_string(filename).unwrap();
        let length = contents.len();

        let response = format!("{status_line}\r\nContent-Length: {length}\r\n\r\n{contents}");

        stream.write_all(response.as_bytes()).unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        handle_connection(stream);
    }
}

fn print_test(input: &str) {
    #[derive(Debug)]
    struct Structure(i32);

    // Put a `Structure` inside of the structure `Deep`. Make it printable
    // also.
    #[derive(Debug)]
    struct Deep(Structure);
    // Printing with `{:?}` is similar to with `{}`.
    println!("{:?} months in a year.", 12);
    println!(
        "{1:?} {0:?} is the {actor:?} name.",
        "Slater",
        "Christian",
        actor = "actor's"
    );

    // `Structure` is printable!
    println!("Now {:?} will print!", Structure(3));

    // The problem with `derive` is there is no control over how
    // the results look. What if I want this to just show a `7`?
    println!("Now {:?} will print!", Deep(Structure(7)));
}

fn pointer_test() {
    let mut x = 5;
    let y1 = Box::new(x);
    let y2 = Box::new(x);
    x = x + 1;
    let y3 = y1;
    print!("{},{},{}\n", x, y3, y2);
    struct Node<T> {
        data: T,
        prew: Option<NodeRef<T>>,
        next: Option<NodeRef<T>>,
    };

    impl<T> Node<T> {
        fn new(data: T) -> Self {
            Self {
                data: data,
                prew: None,
                next: None,
            }
        }
    };

    type NodeRef<T> = Box<Node<T>>;

    let mut n1 = Node::new(5);
    let mut n1p = NodeRef::from(n1);
    n1p.data = 10;
    print!("n1p.data = {}", n1p.data);

    let mut node1 = NodeRef::new(Node {
        data: 1,
        prew: None,
        next: None,
    });

    let mut node2 = NodeRef::new(Node {
        data: 2,
        prew: None,
        next: None,
    });

    let mut node3 = Node::new(41);

    // node1.next = Some(node1);
    // node2.prew = Some(node2);
}

fn thread_test2() {
    let v = vec![1, 2, 3];

    let handle = thread::spawn(move || {
        println!("Here's a vector: {v:?}");
    });

    handle.join().unwrap();
}

fn varibal_test() {
    let a: i32;
    a = 100000000;
    print!("{a}\n");

    let b: i32 = 100;
    let c = a as i64 * b as i64;

    print!("c = {c}\n");
    let mut x: u8 = 123;
    let y = x;
    // x can still be used
    println!("x={}, y={}", x, y);

    let xp1: *mut u8 = &mut x;
    let xp2: *mut u8 = &mut x;
    unsafe {
        print!("{}, {}\n", *xp1, *xp2);
    }
}

fn lifetime_test() {
    fn failed_borrow<'a>() {
        let _x: i32 = 12;
    }

    fn as_str<'a>(data: &'a u32) -> String {
        'b: {
            let s = format!("{}", data);
            return s;
        }
    }

    'c: {
        let x: u32 = 0;

        // An anonymous scope is introduced because the borrow does not
        // need to last for the whole scope x is valid for. The return
        // of as_str must find a str somewhere before this function
        // call. Obviously not happening.
        println!("{}", as_str(&x));
    }

    // let mut data = vec![1, 2, 3];
    // let x = &data[0];
    // data.push(4);
    // println!("{}", x);

    #[derive(Debug)]
    struct X<'a>(&'a i32);

    impl Drop for X<'_> {
        fn drop(&mut self) {}
    }

    let mut data = vec![1, 2, 3];
    let x = X(&data[0]);
    println!("{:?}", x);
    // data.push(4);
}

fn array_test() {
    const NUM: usize = 100;
    let mut a: [i32; NUM] = [0; NUM];
    a[0] = 1;
    print!("{:?}\n", a);
}

fn macro_test() {
    macro_rules! o_O {
        (
            $(
                $x:expr; [ $( $y:expr ),* ]
            );*
        ) => {
            &[ $($( $x + $y ),*),* ]
        }
    }

    let a: &[i32] = o_O!(10; [1, 2, 3];
           20; [4, 5, 6]);
    print!("{:?}\n", a);

    macro_rules! five_times {
        ($x:expr) => {
            5 * $x
        };
    }
    print!("{}", five_times!(2 + 3));

    macro_rules! m1 {
        ($( $x:expr ),*) => {
            print!("x = {:?}\n", $($x,)*);
        };
    }
    m1!(1 + 1);
    m1!((1, 2, 3));
}

fn skip_list_test() {
    use orderbook::skiplist_helper::skiplist_serde;
    use orderbook::skiplist_orderbook::PriceLevel;
    use orderbook::{L3Order, L3OrderRef};
    use ordered_float::OrderedFloat;
    use serde;
    use serde::{Deserialize, Serialize};
    use serde_json::{ser, Result, Value};
    use skiplist::SkipMap;
    use std::cmp::{Ord, Ordering};
    use std::collections::LinkedList;
    use std::collections::VecDeque;
    use std::error::Error;
    #[derive(Serialize, Deserialize, Debug)]
    struct Order {
        pub price: i64,
        pub qrt: i64,
    }

    impl Order {
        fn new(price: i64, qrt: i64) -> Self {
            Self {
                price: price,
                qrt: qrt,
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct Bucket {
        orders: LinkedList<Order>,
    }

    impl Bucket {
        pub fn new() -> Self {
            Self {
                orders: LinkedList::new(),
            }
        }

        pub fn add_order(&mut self, order: Order) {
            self.orders.push_back(order);
        }

        pub fn get_front(&self) -> Option<&Order> {
            return self.orders.front();
        }
    }

    #[derive(Eq, Debug)]
    struct Price {
        price: OrderedFloat<f64>,
        reverse: bool,
    }

    impl Price {
        fn new(price: f64, reverse: Option<bool>) -> Self {
            Self {
                price: OrderedFloat(price),
                reverse: reverse.unwrap_or(false),
            }
        }
    }

    impl Ord for Price {
        fn cmp(&self, other: &Self) -> Ordering {
            match self.reverse {
                true => match self.price.cmp(&other.price) {
                    Ordering::Less => Ordering::Greater,
                    Ordering::Greater => Ordering::Less,
                    Ordering::Equal => Ordering::Equal,
                },
                false => self.price.cmp(&other.price),
            }
        }
    }

    impl PartialOrd for Price {
        fn partial_cmp(&self, other: &Price) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl PartialEq for Price {
        fn eq(&self, other: &Price) -> bool {
            self.price == other.price
        }
    }

    #[derive(Serialize, Deserialize, Debug)]
    struct OrderBook {
        #[serde(with = "skiplist_serde")]
        order_book: SkipMap<i64, Box<Bucket>>,
    }

    impl OrderBook {
        pub fn new() -> Self {
            Self {
                order_book: SkipMap::new(),
            }
        }

        pub fn borrow_mut(&mut self) -> &mut SkipMap<i64, Box<Bucket>> {
            return &mut self.order_book;
        }
    }

    let mut order_book = OrderBook::new();
    let mut map = order_book.borrow_mut();
    fn t1(map: &mut SkipMap<i64, Box<Bucket>>) {
        map.insert(124, Box::new(Bucket::new()));
        map.insert(123, Box::new(Bucket::new()));
        // print!("{}\n", map.len());
        let order = Order::new(100, 100);
        let order2 = Order::new(101, 100);
        let bucket1 = map.get_mut(&123).unwrap();
        bucket1.add_order(order);
        let bucket2 = map.get_mut(&124).unwrap();
        // print!("{:p}\n", &order);
        bucket2.add_order(order2);
    }

    let mut map2: SkipMap<i64, i64> = SkipMap::new();
    let front = map2.front_mut();
    print!("{:?}\n",front);
    // for i in 1..=10 {
    //     map2.insert(i, i + 10);
    // }

    
    // for (k, v) in &mut map2 {
    //     map2.remove(k);
    // }

    // t1(&mut map);
    // loop {
    //     let item = map.front();
    //     match item {
    //         Some((key, value)) => {
    //             map.pop_front();
    //         }
    //         None => {
    //             break;
    //         }
    //     }
    // }
    // print!("map = {:?}\n", map);
    // t1(&mut map);
    // print!("{:?}\n", map.get(&123).unwrap().get_front().unwrap());
    // let serialized_order_book = serde_json::to_string(&order_book).unwrap();
    // println!("serialized_bucket = {:?}", serialized_order_book);
    // let new_order_book: OrderBook = serde_json::from_str(&serialized_order_book).unwrap();
    // print!("new_order_book = {:?}", new_order_book);

    // let mut price_level = PriceLevel::new();

    // for i in 1..=5 {
    //     let order = L3Order::new_ref(i, Side::Buy, 100, 100.0, 100);
    //     price_level.add_order(order.clone());
    // }
    // let orders = &price_level.orders;
    // // let iter = orders.iter().next().unwrap();
    // print!("len = {}, {:?}\n", orders.len(), orders);

    // let order = L3Order::new_ref(100, Side::Buy, 100, 100.0, 100);
    // price_level.add_order(order.clone());
    // print!("order = {:?}\n", order);

    // let mut deque: VecDeque<Option<L3OrderRef>> = VecDeque::new();
    // for i in 1..5 {
    //     let order = L3Order::new_ref(i, Side::Buy, 100, 100.0, 100);
    //     deque.push_back(Some(order.clone()));
    // }
    // deque[2] = None;
    // deque.clear();
    // print!("length = {}, {:?}\n", deque.len(), deque);
    // let serialized_deque = serde_json::to_string(&deque).unwrap();
    // print!("{:?}\n", serialized_deque);

    // let serialized_price_level = serde_json::to_string(&price_level).unwrap();
    // print!("{:?}\n", serialized_price_level);

    // let p1 = Price::new(1.0, Some(true));
    // let p2 = Price::new(2.0, Some(true));
    // print!("{:?}\n", p1.partial_cmp(&p2).unwrap());
    // {
    //     let p1 = Rc::new(Price::new(1.0, Some(true)));
    //     let p2 = p1.clone();
    //     drop(p1);
    //     print!("{:?},{}\n", p2, Rc::strong_count(&p2));
    // }
    // {
    //     let p1 = RefCell::new(Price::new(1.0, Some(true)));
    //     let p2 = p1.borrow();
    //     let p3 = p1.borrow();
    //     print!("{:?}, \n", p1);
    // }
    // {
    //     let p1 = Rc::new(RefCell::new(Price::new(1.0, Some(true))));
    //     let p2 = p1.clone();
    //     print!("{:?}, {}\n", p2, Rc::strong_count(&p1));
    //     let p3 = p2.borrow();
    //     let p4 = p1.borrow();
    // }
    // *p1.price = 2.0;
    // print!("{:?}",p2);

    // let side = Side::Buy;
    // print!("side = {:?}", side);

    // print!("{:?}", map);

    // for (key, value) in map.iter() {
    //     print!("key = {:?}, value = {:?}\n", key, value);
    // }

    // let order = map.get(&123).unwrap().get_front().unwrap();
    // print!("{:p}\n", &order);
    // let serialized = serde_json::to_string(&map).unwrap();
    // println!("serialized = {}", serialized);

    // let mut bucket = Bucket::new();
    // let order = Order::new(100, 100);
    // let order2 = Order::new(101, 100);
    // bucket.add_order(order);
    // bucket.add_order(order2);
    // // print!("{:?}", bucket);
    // let serialized_bucket = serde_json::to_string(&bucket).unwrap();
    // println!("serialized_bucket = {:?}", serialized_bucket);
}

fn float_test() {
    use orderbook::types::{OrdType, Side};
    let price: f64 = 1.253;
    let tick_size = 0.001;
    let price_tick: i64 = (price / tick_size).round() as i64;
    print!("price_tick = {}\n", price_tick);

    let order = Order::new_ref(
        Some("user".to_string()),
        "stock_code".to_string(),
        123,
        100.0,
        100.0,
        "s",
        OrdType::L,
    OrderSourceType::UserOrder,
    );
    fn process_order(order: OrderRef) {
        let order2 = order.clone();
        let order3 = &order;
        // let mut o = RefCell::borrow_mut(&order);
        {
            let mut o = RefCell::borrow_mut(order3);
            o.price = 1000.1123;
        }
        print!(
            "refcount = {}, {:?}\n",
            Rc::strong_count(&order2),
            order2.borrow()
        );
    }

    // process_order(order);
    let mut queue: VecDeque<Option<(i64, i64)>> = VecDeque::new();
    // for i in 1..=10 {
    //     if i == 2 {
    //         queue.push_back(None);
    //     } else {
    //         queue.push_back(Some((i,10)));
    //     }
    // }
    for i in 1..=10 {
        queue.push_back(Some((i%3,i)));
    }

    let mut iter = queue.iter_mut();
    loop {
        let i = iter.next();
        if let Some(v) = i{
            print!("{:?}\n", v);
        }else {
            break;
        }
    }
    // for i in iter{
    //     print!("{:?}\n", i);
    // }

    // print!("{:?}\n", queue);
    // queue.make_contiguous().sort();
    // print!("{:?}\n", queue);
    // let iter = queue.iter_mut();
    // for idx in 0..queue.len() {
    //     match &queue[idx] {
    //         Some(value) => print!("{:?},", queue[idx]),
    //         None => continue,
    //     }
    // }
    // print!("\n");
}



fn main() {
    // test1();
    // loop_test();
    // struct_test();
    // closure_test();
    // iterator_test();
    // smart_pointer_test();
    // refcell_test();
    // thread_test();
    // web_test();
    // print_test();
    // pointer_test();
    // thread_test2();
    // varibal_test()
    // lifetime_test();
    // array_test();
    // macro_test();
    // skip_list_test();
    float_test();
}
