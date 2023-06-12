%{
%}

%token BR EOF INDENT DEDENT MINUS STAR K_IMPORT_CSV K_OPEN_ACCOUNT

%token <int> SPACE
%token <int> INT_LIT
%token <string> ID
%token <string> STRING_LIT

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
| K_OPEN_ACCOUNT date=Date account=Account currency=ID {
  Model.OpenAccount {
    date;
    account;
    currency;
  }
}
| K_IMPORT_CSV filename=STRING_LIT {
  Model.ImportCSV {
    filename;
    transactions = [];
  }
}
| K_IMPORT_CSV filename=STRING_LIT INDENT transactions=Transactions DEDENT {
  Model.ImportCSV {
    filename;
    transactions;
  }
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
| STAR date=Date narration=STRING_LIT INDENT postings=separated_list(BR, Posting) DEDENT {
  Model.make_transaction ~date ~flag:"*" ~narration ~postings ()
}

Posting :
| account=Account amount=ArithExpr {
  Model.make_posting ~account ~amount ()
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
