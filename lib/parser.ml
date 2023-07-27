open Util

let string_of_token = function
  | P.BR -> "BR"
  | P.DEDENT -> "DEDENT"
  | P.EOF -> "EOF"
  | P.ID s -> "ID " ^ s
  | P.INDENT -> "INDENT"
  | P.INT_LIT i -> "INT_LIT " ^ string_of_int i
  | P.K_ASSERT -> "!assert"
  | P.K_IMPORT -> "!import"
  | P.K_OPEN_ACCOUNT -> "!open-account"
  | P.K_SHOW -> "!show"
  | P.MINUS -> "MINUS"
  | P.SPACE n -> "SPACE " ^ string_of_int n
  | P.STAR -> "STAR"
  | P.STRING_LIT s -> "STRING_LIT " ^ s
  | P.TAG s -> "TAG " ^ s

(* Thanks to: https://zehnpaard.hatenablog.com/entry/2019/06/11/090829 *)
module Lexer = struct
  let current_indent = ref 0
  let queued_src_token = ref []

  let peek_src_token lex =
    match !queued_src_token with
    | x :: _ -> x
    | [] ->
        let token = L.main lex in
        queued_src_token := token :: !queued_src_token;
        token

  let next_src_token lex =
    let ret = peek_src_token lex in
    queued_src_token := List.tl !queued_src_token;
    ret

  let main' width lex () =
    let token =
      (* Skip SPACE sequence *)
      let rec aux token =
        match (token, peek_src_token lex) with
        | P.SPACE _, SPACE _ -> aux (next_src_token lex)
        | _ -> token
      in
      aux (next_src_token lex)
    in
    (* Printf.eprintf "Lexer.main': eat %s\n" (string_of_token token); *)
    match token with
    | SPACE n ->
        (* Convert space(s) to indent(s) *)
        let cur = !current_indent in
        let next = n / width in
        current_indent := next;
        if next > cur then repeat (next - cur) [ P.INDENT ]
        else if next < cur then repeat (cur - next) [ P.DEDENT ]
        else [ P.BR ]
    | EOF -> repeat !current_indent [ P.DEDENT ] @ [ P.EOF ]
    | e -> [ e ]

  let cached_tokens = ref []

  let cache f =
    match !cached_tokens with
    | x :: xs ->
        cached_tokens := xs;
        x
    | [] -> (
        match f () with
        | x :: xs ->
            cached_tokens := xs;
            x
        | [] -> assert false)

  let clear () =
    current_indent := 0;
    queued_src_token := [];
    cached_tokens := []

  let main width lex = cache (main' width lex)
end

let do_parse lex =
  let errf_with_pos fmt =
    let pos = lex.Lexing.lex_start_p in
    errf ("%s:%d:%d: " ^^ fmt) pos.pos_fname pos.pos_lnum
      (pos.pos_cnum - pos.pos_bol)
  in
  Lexer.clear ();
  try Ok (P.toplevel (Lexer.main 2) lex) with
  | L.Error s -> errf_with_pos "%s" s
  | P.Error -> errf_with_pos "syntax error"

let parse_string s = do_parse (Lexing.from_string s)

let parse_file filename =
  with_file filename @@ fun ic ->
  let lex = Lexing.from_channel ic in
  Lexing.set_filename lex filename;
  do_parse lex
