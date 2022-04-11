#![feature(map_first_last)]

use crate::advanced_parallel_test::test_all_advanced_parallel;
use crate::advanced_serial_test::test_all_advanced_serial;
use crate::basic_serial_test::test_all_basic_serial;
use crate::paxos_parallel_test::{
  test_all_basic_parallel, test_all_paxos_parallel, ParallelTestStats, Writer,
};
use crate::stats::Stats;
use clap::{arg, App};
use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use runiversal::test_utils::mk_seed;
use std::cmp::max;
use std::collections::BTreeMap;
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
//  Print Utils
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
struct ConcurrentWriter<'a> {
  sender: &'a Sender<ParallelTestMessage>,
  print_buffer: Vec<String>,
}

impl<'a> ConcurrentWriter<'a> {
  fn create(sender: &Sender<ParallelTestMessage>) -> ConcurrentWriter {
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
    self.sender.send(ParallelTestMessage::Error(text));
  }
}

impl<'a> Writer for ConcurrentWriter<'a> {
  fn println(&mut self, s: String) {
    self.print_buffer.push(format!("{}\n", s));
  }

  /// Flushes the currently bufferred string normally.
  fn flush(&mut self) {
    let text = self.mk_text();
    self.sender.send(ParallelTestMessage::PrintMessage(text));
  }
}

// -----------------------------------------------------------------------------------------------
//  Stat Utils
// -----------------------------------------------------------------------------------------------

/// Takes the average of all numbers in `Stats` across the whole vector `all_stats`.
fn process_stats(all_stats: Vec<Stats>) -> (f64, BTreeMap<&'static str, f64>) {
  let num_stats = all_stats.len();

  let mut avg_duration: f64 = 0.0;
  let mut avg_message_stats = BTreeMap::<&'static str, f64>::new();

  // First, take the sum of the desired stats.
  for stats in all_stats {
    avg_duration += stats.duration as f64;
    for (key, count) in stats.get_message_stats() {
      if let Some(cur_count) = avg_message_stats.get_mut(*key) {
        *cur_count += *count as f64;
      } else {
        avg_message_stats.insert(*key, *count as f64);
      }
    }
  }

  // Next, compute the average.
  avg_duration /= num_stats as f64;
  for (_, count) in &mut avg_message_stats {
    *count /= num_stats as f64;
  }

  (avg_duration, avg_message_stats)
}

/// Formats the `message_stats` into a string such that the colons in the map are aligned.
fn format_message_stats(message_stats: BTreeMap<&'static str, f64>) -> String {
  let mut max_key_len = 0;
  for (key, _) in &message_stats {
    max_key_len = max(max_key_len, key.len());
  }

  let mut lines = Vec::<String>::new();
  lines.push("{".to_string());
  for (key, count) in message_stats {
    lines.push(format!(
      "{spaces}{key}: {count},",
      spaces = " ".repeat(max_key_len - key.len() + 4), // We use an indent of 4
      key = key,
      count = count.floor()
    ));
  }
  lines.push("}".to_string());

  lines.join("\n")
}

// -----------------------------------------------------------------------------------------------
//  Parallel Simulation Tests
// -----------------------------------------------------------------------------------------------

/// The message sent from a the test executor threads to the coordinator thread
/// (i.e the main thread).
enum ParallelTestMessage {
  PrintMessage(String),
  Error(String),
  Done((ParallelTestStats, Vec<Stats>)),
}

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
  let (sender, receiver) = mpsc::channel::<ParallelTestMessage>();

  // Create `instances` number of threads to run the test in parallel.
  for _ in 0..instances {
    let seed = mk_seed(rand);
    let sender = sender.clone();
    std::thread::spawn(move || {
      let mut writer = ConcurrentWriter::create(&sender);
      let mut rand = XorShiftRng::from_seed(seed);

      // Catch any panics or errors that happen inside
      let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        println!("Paxos Parallel Tests:");
        let parallel_stats = test_all_paxos_parallel(&mut rand, &mut writer);
        println!("\n");
        println!("Basic Parallel Tests:");
        let stats_basic = test_all_basic_parallel(&mut rand, &mut writer);
        println!("\n");

        (parallel_stats, stats_basic)
      }));

      // If the above ended with an error, we flush the last of whatever was  written
      // as an error. Otherwise, we flush it normally and send off the results.
      match result {
        Ok(done) => {
          writer.flush();
          sender.send(ParallelTestMessage::Done(done));
        }
        Err(_) => writer.flush_error(),
      }
    });
  }

  // Drop the original sender to avoid blocking the following `recv` call forever.
  std::mem::drop(sender);

  let mut parallel_stats_acc = Vec::<ParallelTestStats>::new();
  let mut basic_stats_acc = Vec::<Vec<Stats>>::new();

  // Receive data until there are no more `senders` in existance; i.e. when all
  // threads above have finished.
  while let Ok(result) = receiver.recv() {
    match result {
      ParallelTestMessage::PrintMessage(string) => println!("{}", string),
      ParallelTestMessage::Error(string) => {
        println!("{}", string);
        println!("Terminating...");
        // Terminate all testing.
        return;
      }
      ParallelTestMessage::Done((parallel_stats, basic_stats)) => {
        parallel_stats_acc.push(parallel_stats);
        basic_stats_acc.push(basic_stats);
      }
    }
  }

  // Process the basic stats
  {
    let mut all_stats = Vec::<Stats>::new();

    for basic_stats in basic_stats_acc {
      all_stats.extend(basic_stats);
    }

    let (avg_duration, avg_message_stats) = process_stats(all_stats);

    // Print the stats.
    println!("Avg Basic Duration: {}", avg_duration);
    println!("Avg Basic Statistics: {}", format_message_stats(avg_message_stats));
  }

  // Process the parallel stats
  {
    let mut all_stats = Vec::<Stats>::new();
    let mut all_reconfig_stats = Vec::<Stats>::new();

    for parallel_stats in parallel_stats_acc {
      all_stats.extend(parallel_stats.all_stats);
      all_reconfig_stats.extend(parallel_stats.all_reconfig_stats);
    }

    let (avg_duration, avg_message_stats) = process_stats(all_stats);
    let (avg_reconfig_duration, avg_reconfig_message_stats) = process_stats(all_reconfig_stats);

    // Print the stats.
    println!("Avg Duration: {}", avg_duration);
    println!("Avg Statistics: {}", format_message_stats(avg_message_stats));
    println!("Avg Reconfig Duration: {}", avg_reconfig_duration);
    println!("Avg Reconfig Statistics: {}", format_message_stats(avg_reconfig_message_stats));
  }
}
