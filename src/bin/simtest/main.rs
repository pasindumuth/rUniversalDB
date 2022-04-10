#![feature(map_first_last)]

use crate::advanced_parallel_test::test_all_advanced_parallel;
use crate::advanced_serial_test::test_all_advanced_serial;
use crate::basic_serial_test::test_all_basic_serial;
use crate::paxos_parallel_test::{test_all_basic_parallel, test_all_paxos_parallel, Writer};
use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::test_utils::mk_seed;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc;
use std::sync::mpsc::Sender;

#[macro_export]
macro_rules! cast {
  ($enum:path, $expr:expr) => {{
    if let $enum(item) = $expr {
      Ok(item)
    } else {
      Err("Could not cast the value to the desired Variant.")
    }
  }};
}

mod advanced_parallel_test;
mod advanced_serial_test;
mod basic_serial_test;
mod paxos_parallel_test;
mod serial_test_utils;
mod simulation;
mod stats;

/**
 * Debugging Tips:
 *  - We thread a global RNG through all test cases. However, in every test case, we try to
 *    use it for nothing more than creating a new RNG by creating a random seed. The reason
 *    for this is so that if a failure happens, we can just print the seed and then quickly
 *    reproduce by using that seed directly to run the test case.
 */

fn main() {
  // Setup CLI parsing
  let matches = App::new("rUniversalDB Tests")
    .version("1.0")
    .author("Pasindu M. <pasindumuth@gmail.com>")
    .arg(
      arg!(-i --instances <VALUE>)
        .required(false)
        .help("Indicates if the simulation tests should be run in parallel."),
    )
    .get_matches();

  // Run Serial tests in just one thread (since these are fast).
  let mut rand = XorShiftRng::from_seed([1; 16]);
  println!("Basic Serial Tests:");
  test_all_basic_serial(&mut rand);
  println!("\n");
  println!("Advanced Serial Tests:");
  test_all_advanced_serial(&mut rand);
  println!("\n");

  // Run parallel tests, potentially in multiple threads if requested.
  if let Some(instances) = matches.value_of("instances") {
    let instances: u32 = instances.parse().unwrap();
    execute_multi(instances, &mut rand);
  } else {
    execute_once(&mut rand)
  }

  // TODO: this test grinds to a halt when we use the many-messages delivery scheme.
  // println!("\n");
  // println!("Advanced Parallel Tests:");
  // test_all_advanced_parallel(&mut rand);
}

// -----------------------------------------------------------------------------------------------
//  Utils
// -----------------------------------------------------------------------------------------------

/// Trivial implementation just using `println!`.
struct BasicPrintWriter {}

impl Writer for BasicPrintWriter {
  fn println(&mut self, s: String) {
    println!("{}", s);
  }

  fn flush(&mut self) {}
}

/// Concurrent Writer for when we want multiple threads writing data. This class allows multiple
/// `println` calls to be batched together and then written to the console atomically with
/// `flush`. We also have `flush_error` so that if the thread errors out before it would normally
/// call `flush`, then we can catch the exception and then call this function explicitly.
struct ConcurrentWriter {
  sender: Sender<Result<String, String>>,
  print_buffer: Vec<String>,
}

impl ConcurrentWriter {
  fn create(sender: Sender<Result<String, String>>) -> ConcurrentWriter {
    ConcurrentWriter { sender, print_buffer: vec![] }
  }

  fn mk_text(&mut self) -> String {
    let print_buffer = std::mem::take(&mut self.print_buffer);
    print_buffer.join("")
  }

  /// Flushes the currently bufferred string as an error, indicating that the sending
  /// thread encountered an error.
  fn flush_error(&mut self) {
    let text = self.mk_text();
    self.sender.send(Err(text));
  }
}

impl Writer for ConcurrentWriter {
  fn println(&mut self, s: String) {
    self.print_buffer.push(format!("{}\n", s));
  }

  /// Flushes the currently bufferred string normally.
  fn flush(&mut self) {
    let text = self.mk_text();
    self.sender.send(Ok(text));
  }
}

// -----------------------------------------------------------------------------------------------
//  Parallel Simulation Tests
// -----------------------------------------------------------------------------------------------

/// Execute parallel tests in a single thread.
fn execute_once(rand: &mut XorShiftRng) {
  let mut writer = BasicPrintWriter {};
  println!("Paxos Parallel Tests:");
  test_all_paxos_parallel(rand, &mut writer);
  println!("\n");
  println!("Basic Parallel Tests:");
  test_all_basic_parallel(rand, &mut writer);
  println!("\n");
}

/// Execute parallel tests in multiple threads.
fn execute_multi(instances: u32, rand: &mut XorShiftRng) {
  let (sender, receiver) = mpsc::channel::<Result<String, String>>();

  // Create `instances` number of threads to run the test in parallel.
  for _ in 0..instances {
    let seed = mk_seed(rand);
    let sender = sender.clone();
    std::thread::spawn(move || {
      let mut writer = ConcurrentWriter::create(sender);
      let mut rand = XorShiftRng::from_seed(seed);

      // Catch any panics or errors that happen inside
      let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        println!("Paxos Parallel Tests:");
        test_all_paxos_parallel(&mut rand, &mut writer);
        println!("\n");
        println!("Basic Parallel Tests:");
        test_all_basic_parallel(&mut rand, &mut writer);
        println!("\n");
      }));

      // If the above ended with an error, we flush the last of
      // whatever was written as an error.
      if result.is_err() {
        writer.flush_error();
      }
    });
  }

  // Drop the original sender to avoid blocking the following `recv` call forever.
  std::mem::drop(sender);

  // Receive data until there are no more `senders` in existance; i.e. when all
  // threads above have finished.
  while let Ok(result) = receiver.recv() {
    match result {
      Ok(string) => {
        println!("{}", string);
      }
      Err(string) => {
        println!("{}", string);
        break;
      }
    }
  }
}
