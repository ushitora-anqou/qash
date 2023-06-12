open Util

type account = string list
type amount = int
type cost = int
type price = int

type posting = {
  flag : string option;
  account : account;
  amount : amount;
  cost : cost option;
  price : price option;
  narration : string option;
}
[@@deriving make]

type date = { year : int; month : int; day : int } [@@deriving make]

type transaction = {
  date : date;
  flag : string;
  payee : string option;
  narration : string option;
  postings : posting list;
}
[@@deriving make]

type directive =
  | OpenAccount of { date : date; account : account; currency : string }
  | Transaction of transaction
  | ImportCSV of { filename : string; transactions : transaction list }

let account_of_string : string -> account = String.split_on_char ':'
let string_of_date d = Printf.sprintf "%04d-%02d-%02d" d.year d.month d.day

let string_of_posting p =
  Printf.sprintf "%s %d" (p.account |> String.concat ":") p.amount

let string_of_transaction t =
  Printf.sprintf "* %s %s\n  %s" (string_of_date t.date)
    (t.narration |> Option.fold ~none:"" ~some:(fun s -> "\"" ^ s ^ "\""))
    (t.postings |> List.map string_of_posting |> String.concat "\n  ")

let amount_of_string : string -> amount =
  String.split_on_char ',' *> String.concat "" *> int_of_string
