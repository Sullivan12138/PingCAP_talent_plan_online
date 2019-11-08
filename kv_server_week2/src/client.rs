use lib::kv_client::Client;
use std::io::stdin;
fn main() {
    let test_host = String::from("127.0.0.1");
    let test_port = 20001;
    let client = Client::new(test_host.clone(), test_port);
    loop {
        println!("Please choose one operation:");
        println!("0. Exit this system");
        println!("1. Get");
        println!("2. Set");
        println!("3. Delete");
        println!("4. Scan");
        let mut choice = String::new();

        stdin().read_line(&mut choice);
        let choice = choice.trim();
        println!("{:?}", choice);
        match choice {
            "0" => {
                println!("Thanks for using.");
                break;
            }
            "1" => {
                println!("Please input the key:");
                let mut key = String::new();
                stdin().read_line(&mut key);
                let key = key.trim();
                let ret = client.get(key.to_owned());
                match ret {
                    Some(value) => {
                        println!("Get successfully! The value is {}", value);
                    }
                    None => {
                        println!("Sorry, the key isn't found in database, please check your input or insert it.");
                    }
                }
            }
            "2" => {
                println!("Please input the key:");
                let mut key = String::new();
                stdin().read_line(&mut key);
                let key = key.trim();
                println!("Please input the value:");
                let mut value = String::new();
                stdin().read_line(&mut value);
                let value = value.trim();
                let ret = client.put(key.to_owned(), value.to_owned());
                match ret {
                    true => {
                        println!("Set successfully!");
                    }
                    false => {
                        println!("Set failed.");
                    }
                }
            }
            "3" => {
                println!("Please input the key:");
                let mut key = String::new();
                stdin().read_line(&mut key);
                let key = key.trim();
                let ret = client.delete(key.to_owned());
                match ret {
                    true => {
                        println!("Delete successfully!");
                    }
                    false => {
                        println!("Delete failed");
                    }
                }
            }
            "4" => {
                println!("Please input the start key:");
                let mut start_key = String::new();
                stdin().read_line(&mut start_key);
                let start_key = start_key.trim();
                println!("Please input the end key:");
                let mut end_key = String::new();
                stdin().read_line(&mut end_key);
                let end_key = end_key.trim();
                if start_key > end_key {
                    println!("Error, the end key is smaller than the start key!");
                    continue;
                }
                let ret = client.scan(start_key.to_owned(), end_key.to_owned());
                match ret {
                    Some(map) => {
                        println!("Scan successfully!");
                        for (k, v) in map.iter() {
                            println!("key: {}, value: {}", k, v);
                        }
                    }
                    None => {
                        println!("Sorry, the range you want doesn't exist in the database, please check your input.");
                    }
                }
            }
            _ => {
                println!("Please input a number between 0 and 4!");
            }
        }
    }
}
