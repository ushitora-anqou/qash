{
exception Error of string
}

let indent = '\n' ' '*
let whitespace = [ ' ' '\t' ]

rule main = parse
| indent as s {
  Lexing.new_line lexbuf;
  P.SPACE (String.length s - 1)
}
| whitespace+ {
  main lexbuf
}
| "(*" {
  comment lexbuf;
  main lexbuf
}
| [ '0'-'9' ][ '0'-'9' ',' ]* as s {
  P.INT_LIT (Model.amount_of_string s)
}
| '"' ([ ^ '"' ]* as s) '"' {
  P.STRING_LIT s
}
| '-' {
  P.MINUS
}
| '*' {
  P.STAR
}
| '!' [ 'a'-'z' 'A'-'Z' '0'-'9' '_' '-' ]+ as s {
  match s with
  | "!import-csv" -> P.K_IMPORT_CSV
  | "!open-account" -> P.K_OPEN_ACCOUNT
  | _ -> raise (Error ("unknown keyword: " ^ s))
}
| [ ^ '!' ' ' '\t' '\n' '0'-'9' '-' ] [ ^ ' ' '\t' '\n' ]+ {
  let id = Lexing.lexeme lexbuf in
  P.ID id
}
| eof {
  P.EOF
}
| _ as c {
  raise (Error ("unexpected char: " ^ (String.make 1 c)))
}

and comment = parse
| "(*" {
  comment lexbuf;
  comment lexbuf
}
| "*)" {
  ()
}
| _ {
  comment lexbuf
}