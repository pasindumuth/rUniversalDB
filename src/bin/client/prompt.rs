use crossterm::cursor::{MoveLeft, MoveRight, MoveToColumn};
use crossterm::event::{read, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode};
use std::io::{stdout, Write};

/// The caret to print at the front.
const PROMPT_STR: &str = "> ";

pub struct CommandPrompt {
  history: Vec<Vec<char>>,
  /// This points to the current element in the history that the user is pointing
  /// to (as a consequence of pressing the `up` and `down` arrow keys). This is
  /// reset after a command is submitted.
  index: usize,

  /// Data indicating the current line.
  line: Vec<char>,
  /// This points to an element in `line` (or one element after).
  pos: usize,

  /// The numbers of characters (excluding the caret) that were last rendered.
  last_render_length: usize,
}

impl CommandPrompt {
  pub fn new() -> CommandPrompt {
    CommandPrompt { history: Vec::new(), index: 0, line: vec![], pos: 0, last_render_length: 0 }
  }

  fn render_line(&mut self) -> crossterm::Result<()> {
    // Clear the current line.
    execute!(stdout(), MoveToColumn((1 + PROMPT_STR.len()) as u16))?;
    for _ in 0..self.last_render_length {
      print!(" ");
    }

    // Print the line.
    execute!(stdout(), MoveToColumn((1 + PROMPT_STR.len()) as u16))?;
    for c in &self.line {
      print!("{}", c);
    }

    // Set the cursor position.
    execute!(stdout(), MoveToColumn((1 + PROMPT_STR.len()) as u16))?;
    execute!(stdout(), MoveRight(self.pos as u16))?;

    // Set the rendering length.
    self.last_render_length = self.line.len();

    stdout().flush()
  }

  fn submit(&mut self) -> crossterm::Result<Vec<char>> {
    // Create a new line, empty line and move the cursor to the start (no caret).
    self.pos = 0;
    let line = std::mem::take(&mut self.line);
    execute!(stdout(), MoveToColumn(1))?;
    print!("\n");
    stdout().flush()?;

    // Return the line that just finished.
    Ok(line)
  }

  fn prompt(&mut self) -> crossterm::Result<Vec<char>> {
    // Print prompt
    print!("{}", PROMPT_STR);
    stdout().flush().unwrap();

    loop {
      // Listen for a key event.
      let event = read()?;
      if let Event::Key(key_event) = event {
        match key_event.code {
          KeyCode::Backspace => {
            if self.pos > 0 {
              self.pos -= 1;
              self.line.remove(self.pos);
              self.render_line();
            }
          }
          KeyCode::Enter => {
            break self.submit();
          }
          KeyCode::Left => {
            if self.pos > 0 {
              self.pos -= 1;
              self.render_line();
            }
          }
          KeyCode::Right => {
            if self.pos < self.line.len() {
              self.pos += 1;
              self.render_line();
            }
          }
          KeyCode::Up => {
            if self.index > 0 {
              self.index -= 1;

              // Update the current line with the historical line.
              self.line = self.history.get(self.index).unwrap().clone();
              self.pos = self.line.len();

              self.render_line();
            }
          }
          KeyCode::Down => {
            if self.index < self.history.len() {
              self.index += 1;

              // Update the current line with the historical line.
              if self.index < self.history.len() {
                self.line = self.history.get(self.index).unwrap().clone();
                self.pos = self.line.len();
              } else {
                // If we are pointing past the history, simply start a new, empty line.
                self.line = "".chars().collect();
                self.pos = 0;
              }

              self.render_line();
            }
          }
          KeyCode::Char(c) => {
            self.line.insert(self.pos, c);
            self.pos += 1;
            self.render_line();
          }
          KeyCode::Esc => {
            self.line = "exit".chars().collect();
            self.pos = self.line.len();
            break self.submit();
          }
          _ => {}
        }
      }
    }
  }

  pub fn prompt_wrapper(&mut self) -> crossterm::Result<String> {
    // By default, the terminal does some nice stuff. In order to get raw input
    // and have the terminal do nothing more than the commands we give it with stdout,
    // we switch to raw mode. This way, `read()?` above is immediate.
    enable_raw_mode()?;
    let ret = self.prompt();
    disable_raw_mode()?;

    // Update the history and reset the index.
    let command = ret?;
    self.history.push(command.clone());
    self.index = self.history.len();

    // Return the command
    Ok(command.into_iter().collect())
  }
}
