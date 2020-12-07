%%
SELECT                  "SELECT"
FROM                    "FROM"
,                       ","
[a-zA-Z][a-zA-Z0-9_]*   "identifier"
[0-9]+                  "INT"
\+                      "+"
\*                      "*"
\(                      "("
\)                      ")"
[\t ]+      ;
