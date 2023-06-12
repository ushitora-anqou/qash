open Qash

let test_case1 () =
  let prog =
    {|
!open-account 1871-06-27 資産:流動資産:現金 JPY

* 2014-05-05 "信販C ラクテンカ-ドサ-ビ"
  資産:流動資産:南都銀行普通預金  -400,000
  負債:楽天クレジットカード                  400,000
* 2023-05-08 "セブンイレブン"
  資産:流動資産:現金  -502
  費用:食費           502

* 2023-05-08 "セブンイレブン"
  資産:流動資産:現金  -502
  費用:食費           502

!import-csv "moneyforward/202305.csv"

!import-csv "moneyforward/202306.csv"
  * 2014-05-05 "信販C ラクテンカ-ドサ-ビ"
    資産:流動資産:南都銀行普通預金            -400,000
    負債:楽天クレジットカード                  400,000
  * 2014-05-05 "信販C ラクテンカ-ドサ-ビ"
    資産:流動資産:南都銀行普通預金            -400,000
    負債:楽天クレジットカード                  400,000
  
  * 2023-05-08 "セブンイレブン"
    資産:流動資産:現金  -502
    費用:食費            502

  * 2023-05-08 "セブンイレブン"
    資産:流動資産:現金  -502
    費用:食費            502
|}
  in
  let expected_open_account1 =
    let open Model in
    OpenAccount
      {
        date = make_date ~year:1871 ~month:6 ~day:27;
        account = [ "資産"; "流動資産"; "現金" ];
        currency = "JPY";
      }
  in
  let expected_transaction1 =
    let open Model in
    {
      date = make_date ~year:2014 ~month:5 ~day:5;
      flag = "*";
      narration = Some "信販C ラクテンカ-ドサ-ビ";
      payee = None;
      postings =
        [
          make_posting
            ~account:[ "資産"; "流動資産"; "南都銀行普通預金" ]
            ~amount:(-400000) ();
          make_posting ~account:[ "負債"; "楽天クレジットカード" ] ~amount:400000 ();
        ];
    }
  in
  let expected_transaction2 =
    let open Model in
    {
      date = make_date ~year:2023 ~month:5 ~day:8;
      flag = "*";
      narration = Some "セブンイレブン";
      payee = None;
      postings =
        [
          make_posting ~account:[ "資産"; "流動資産"; "現金" ] ~amount:(-502) ();
          make_posting ~account:[ "費用"; "食費" ] ~amount:502 ();
        ];
    }
  in
  let expected_import_csv1 =
    let open Model in
    ImportCSV { filename = "moneyforward/202305.csv"; transactions = [] }
  in
  let expected_import_csv2 =
    let open Model in
    ImportCSV
      {
        filename = "moneyforward/202306.csv";
        transactions =
          [
            expected_transaction1;
            expected_transaction1;
            expected_transaction2;
            expected_transaction2;
          ];
      }
  in
  let expected =
    [
      expected_open_account1;
      Transaction expected_transaction1;
      Transaction expected_transaction2;
      Transaction expected_transaction2;
      expected_import_csv1;
      expected_import_csv2;
    ]
  in
  match Parser.parse_string prog with
  | Ok actual when expected = actual -> ()
  | Ok _ ->
      Printf.eprintf "unexpected result\n";
      assert false
  | Error s ->
      Printf.eprintf "%s\n" s;
      assert false

let () =
  let open Alcotest in
  run "parser" [ ("basics", [ test_case "case1" `Quick test_case1 ]) ]
