%{
%}

%token BR EOF INDENT DEDENT MINUS STAR K_IMPORT K_OPEN_ACCOUNT K_ASSERT K_SHOW

%token <int> SPACE
%token <int> INT_LIT
%token <string> ID
%token <string> STRING_LIT
%token <string> TAG

%start toplevel
%type <Model.directive list> toplevel
%%

toplevel :
| x=Toplevel {
  x
}

Toplevel :
| BR* x=Directive xs=Toplevel {
  x :: xs
}
| BR* EOF {
  []
}

Date :
| year=INT_LIT MINUS month=INT_LIT MINUS day=INT_LIT {
  Model.{ year; month; day }
}

Directive :
(* !open-account *)
| K_OPEN_ACCOUNT kind=ID account=Account currency=ID {
  Model.OpenAccount {
    account;
    currency;
    kind =
      (
        match kind with
        | "asset" -> Model.Asset
        | "liability" -> Liability
        | "equity" -> Equity
        | "income" -> Income
        | "expense" -> Expense
        | _ -> failwith "invalid account kind"
      );
  }
}
| K_IMPORT filename=STRING_LIT {
  Model.Import {
    filename;
    transactions = [];
  }
}
| K_IMPORT filename=STRING_LIT INDENT transactions=Transactions DEDENT {
  Model.Import {
    filename;
    transactions;
  }
}
| K_ASSERT sql=STRING_LIT {
  Model.Assert sql
}
| K_SHOW sql=STRING_LIT {
  Model.Show sql
}
(* Transaction *)
| x=Transaction {
  Model.Transaction x
}

Transactions :
| BR* x=Transaction xs=Transactions {
  x :: xs
}
| BR* x=Transaction {
  [x]
}

Transaction :
| STAR date=Date narration=STRING_LIT tags=list(TAG)
  postings=option(INDENT ps=separated_list(BR, Posting) DEDENT { ps }) {
  let postings = Option.value ~default:[] postings in
  Model.make_transaction ~date ~narration ~postings ~tags ()
}

Posting :
| account=Account amount=option(ArithExpr) {
  Model.make_posting ~account ?amount ()
}

ArithExpr :
| i=INT_LIT {
  i
}
| MINUS i=INT_LIT {
  -i
}

Account :
| id=ID {
  String.split_on_char ':' id
}
