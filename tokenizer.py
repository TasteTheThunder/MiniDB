from visualizer import print_trace
import config


KEYWORDS = {

"CREATE","TABLE","PRIMARY","KEY",

"INSERT","INTO","VALUES",

"SELECT","FROM","WHERE",

"DELETE","DROP",

"UPDATE","SET",

"GROUP","BY",

"ORDER",

"ASC","DESC",

"LIMIT",

"ALTER","COLUMN","MODIFY","ADD","RENAME","TO","CONSTRAINT",

"SHOW","TABLES","DESCRIBE","TRUNCATE"

}


def tokenize(query):

    tokens = []

    current = ""

    in_quotes = False

    quote_char = None

    i = 0


    while i < len(query):

        ch = query[i]

        # ---------- STRING START ----------

        if ch in ['"',"'"]:

            if not in_quotes:

                in_quotes = True

                quote_char = ch

                current += ch

            else:

                current += ch

                if ch == quote_char:

                    tokens.append(current)

                    current = ""

                    in_quotes = False

            i += 1

            continue


        # ---------- INSIDE STRING ----------

        if in_quotes:

            current += ch

            i += 1

            continue


        # ---------- SYMBOLS ----------

        if ch in "()=,;":

            if current:

                tokens.append(current)

                current = ""

            if ch != ";":

                tokens.append(ch)

            i += 1

            continue


        # ---------- SPACE ----------

        if ch.isspace():

            if current:

                tokens.append(current)

                current = ""

            i += 1

            continue


        current += ch

        i += 1


    if current:

        tokens.append(current)


    # ---------- KEYWORD UPPER ----------

    tokens = [

        t.upper() if t.upper() in KEYWORDS else t

        for t in tokens

    ]


    if config.get_mode() == "EDUCATIONAL":

        print_trace(

            "TOKENIZER",

            [

                "Breaking query into tokens",

                "Tokens Generated:",

                " | ".join(tokens)

            ]

        )

    return tokens