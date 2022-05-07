use crossterm::cursor::{MoveLeft, MoveRight};
use crossterm::event::{read, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use std::io::{stdout, Write};

fn prompt(prompt_str: &str) -> crossterm::Result<Option<String>> {
  // Print prompt
  print!("{}", prompt_str);
  stdout().flush().unwrap();

  // The left-most character in the rest of the line is the latest element here.
  let mut prev = Vec::<char>::new();
  let mut rest = Vec::<char>::new();
  loop {
    // It's guaranteed that read() wont block if `poll` returns `Ok(true)`
    let event = read()?;
    if let Event::Key(key_event) = event {
      match key_event.code {
        KeyCode::Backspace => {
          if let Some(_) = prev.pop() {
            if !rest.is_empty() {
              execute!(stdout(), MoveRight(rest.len() as u16))?;
              execute!(stdout(), MoveLeft(1))?;
              print!(" ");
              execute!(stdout(), MoveLeft(rest.len() as u16))?;
            }
            execute!(stdout(), MoveLeft(1))?;
            print!(" ");
            execute!(stdout(), MoveLeft(1))?;
          }
        }
        KeyCode::Enter => {
          prev.extend(rest.into_iter().rev());
          execute!(stdout(), MoveLeft((prev.len() + prompt_str.len()) as u16))?;
          print!("\n");
          stdout().flush().unwrap();
          break Ok(Some(prev.into_iter().collect()));
        }
        KeyCode::Left => {
          if let Some(c) = prev.pop() {
            rest.push(c);
            execute!(stdout(), MoveLeft(1))?;
          }
        }
        KeyCode::Right => {
          if let Some(c) = rest.pop() {
            prev.push(c);
            execute!(stdout(), MoveRight(1))?;
          }
        }
        KeyCode::Tab => {
          prev.push('\t');
          print!("{}", '\t');
        }
        KeyCode::Char(c) => {
          prev.push(c);
          print!("{}", c);
        }
        KeyCode::Esc => {
          break Ok(None);
        }
        _ => {}
      }

      // Print `rest`, which would be non-empty if the cursor is in the middle
      // of some text.
      for c in rest.iter().rev() {
        print!("{}", c);
      }
      execute!(stdout(), MoveLeft(rest.len() as u16))?;
      stdout().flush().unwrap();
    }
  }
}

pub fn prompt_wrapper() -> crossterm::Result<Option<String>> {
  // By default, the terminal does some nice stuff. In order to get raw input
  // and have the terminal do nothing more than the commands we give it with stdout,
  // we switch to raw mode. This way, `read()?` above is immediate.
  enable_raw_mode()?;
  let ret = prompt("> ");
  disable_raw_mode()?;
  ret
}
