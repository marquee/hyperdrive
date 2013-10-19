


class Set(object):

    # def __init__(self, *args):
        # self._computation = {
        #     "|" : set([]),
        #     "&" : set([]),
        #     "-" : set([])
        # }


    #     if len(args) != 1:
    #         raise Exception("Too many args")

    #     self.setkey = args[0]
    #     # self._computation.update({"|": args})

    # def __repr__(self):
    #     return repr(self._computation)
        

    # def __or__(self, other):
    #     self.setkey + " | " + other.setkey
    #     return self

    # def __sub__(self, other):
    #     self.setkey + " - " + other.setkey
    #     return self

    # def __and__(self, other):
    #     self.setkey + " & " + other.setkey
    #     return self

    @classmethod
    def select(cls, **kwargs):
        sets = []

        computation = {
            "&" : [],
            "-" : []
        }        

        OR = lambda x,y : x + " | " + y

        AND = lambda x,y : x + " & " + y

        skey = lambda p, v: "{}:{}".format(p, v)

        comp = ""

        selected_sets = []
        excluded_sets = []
        for field, value in kwargs.items():
            queryitems = field.split("__")
            param      = queryitems[0]

            if len(queryitems) == 2:
                operator   = queryitems[1]
                
                if operator == "in":    # OR S & (A | B)
                    keys = map(lambda v: skey(param,v), value)

                    selected_sets.append(reduce(OR, keys))

                    # print computation["&"]

                elif operator == "nin": # S - (A | B)
                    keys = map(lambda v: skey(param,v), value)
                    excluded_sets.append(reduce(OR, keys))

                elif operator == "ne":  # S - A
                    excluded_sets += [skey(param, value)]

            else:
                selected_sets += [skey(param, value)] # S & A  


        computation = " ( " + " & ".join(selected_sets) + " ) "
        if excluded_sets:
            computation += "- ( " + " | ".join(excluded_sets) + " ) "
 
        return computation


computation = Set.select(
    category   = "movies", # | movies
    category__ne = "tv", # - tv
    tags__in   = ["action", "comedy"], # & (action | comedy)
    tags__nin  = ["thriller", "romance"],
    language   = "french",
    byline__ne = "huxley"
)

"((category:movies) & (tags:action | tags:comedy) & (language:french)) - (byline:huxley | tags:thriller | tags:romance)"


# print Set("tags:tool") | Set("tags:food") - Set("tags:action")
print computation