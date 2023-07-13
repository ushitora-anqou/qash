open Util

type account = string list

let yojson_of_account a = `String (String.concat ":" a)

let account_of_yojson = function
  | `String s -> String.split_on_char ':' s
  | _ -> failwith "account_of_yojson: expected string"

type amount = int [@@deriving yojson]
type cost = int [@@deriving yojson]
type price = int [@@deriving yojson]

type posting = {
  account : account;
  amount : amount;
  cost : cost option; [@yojson.option]
  price : price option; [@yojson.option]
  narration : string; [@default ""] [@yojson_drop_default ( = )]
  balance : int; [@default 0]
}
[@@deriving make, yojson]

type date = { year : int; month : int; day : int } [@@deriving make, yojson]
type tag = string [@@deriving yojson]

type transaction = {
  date : date;
  narration : string;
  postings : posting list;
  tags : tag list;
}
[@@deriving make, yojson]

type transactions = transaction list [@@deriving yojson]

type open_account = { account : account; currency : string }
[@@deriving make, yojson]

type directive =
  | OpenAccount of open_account
  | Transaction of transaction
  | Import of { filename : string; transactions : transaction list }
[@@deriving yojson]

type directives = directive list [@@deriving yojson]
type t = { accounts : open_account list; transactions : transactions }

let is_date_earlier date ~than =
  let { year; month; day } = date in
  let { year = year'; month = month'; day = day' } = than in
  year < year'
  || (year = year' && month < month')
  || (year = year' && month = month' && day < day')

let date_compare lhs rhs =
  compare [ lhs.year; lhs.month; lhs.day ] [ rhs.year; rhs.month; rhs.day ]

let account_of_string : string -> account = String.split_on_char ':'
let string_of_date d = Printf.sprintf "%04d-%02d-%02d" d.year d.month d.day
let string_of_account : account -> string = String.concat ":"

let string_of_posting (p : posting) =
  Printf.sprintf "%s %d" (string_of_account p.account) p.amount

let amount_of_string : string -> amount =
  String.split_on_char ',' *> String.concat "" *> int_of_string

let pp_transaction ppf t =
  Format.fprintf ppf "@[<v 2>* %s %s@,%a@]" (string_of_date t.date)
    ({|"|} ^ t.narration ^ {|"|})
    (Format.pp_print_list (fun ppf p ->
         Format.fprintf ppf "%s" (string_of_posting p)))
    t.postings

let pp_directive ppf = function
  | OpenAccount { account; currency } ->
      Format.fprintf ppf "!open-account %s %s\n"
        (string_of_account account)
        currency
  | Transaction t ->
      pp_transaction ppf t;
      Format.pp_print_newline ppf ()
  | Import { filename; transactions } ->
      Format.fprintf ppf "@[<v 2>!import \"%s\"@,%a@]\n" filename
        (Format.pp_print_list pp_transaction)
        transactions

let string_of_directives ds =
  let buf = Buffer.create 0 in
  let ppf = Format.formatter_of_buffer buf in
  Format.fprintf ppf "@[<v 0>%a@]@?" (Format.pp_print_list pp_directive) ds;
  Buffer.contents buf |> String.trim

let to_string (t : t) =
  (t.accounts |> List.map (fun a -> OpenAccount a))
  @ (t.transactions |> List.map (fun t -> Transaction t))
  |> string_of_directives

let date_of_string s =
  Scanf.sscanf s "%d-%d-%d" (fun year month day -> make_date ~year ~month ~day)
