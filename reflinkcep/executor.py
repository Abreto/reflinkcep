import logging

from reflinkcep.DST import DST, Configuration
from reflinkcep.ast import QueryContext
from reflinkcep.event import Event, EventStream, Stream

Match = dict[str, EventStream]
MatchStream = Stream[Match]

logger = logging.getLogger(__name__)


class AfterMatchStrategy(str):
    @classmethod
    def from_context(cls, ctx: QueryContext) -> "AfterMatchStrategy":
        strategy = "NoSkip"
        if "strategy" in ctx:
            strategy: str = ctx["strategy"]
        return cls(strategy)


class Executor:
    def __init__(self, dst: DST, strategy: AfterMatchStrategy) -> None:
        self.dst = dst
        self.strategy = strategy

    def reset(self):
        self.S = []
        self.i = 0

    def current_tuple(self, conf: Configuration) -> tuple[int, Configuration]:
        return tuple((self.i, conf))

    def feed(self, event: Event) -> Stream[Match]:
        logger.debug("Feed with %s", event)
        dst = self.dst
        self.i += 1

        T = self.S.copy()
        self.S.clear()
        T.append(self.current_tuple(dst.initial_configuration()))

        i = 0
        n = len(T)
        while i < n:
            k, conf = T[i]
            # print("At ", conf, sep="", flush=True)
            logger.debug("current T: %s", T)
            logger.debug("At %d, %s", k, conf)
            # print(k, i, n, sep=" ")
            i += 1

            q = conf.get_state()
            for edge in dst.start_from(q):
                logger.debug("trying edge %s", edge)
                if edge.predict(conf, event):
                    new_conf = edge.advance(conf, event)
                    logger.debug("now go ahead %s", new_conf)
                    new_tuple = self.current_tuple(new_conf)
                    if edge.is_epsilon():
                        T.insert(i, new_tuple)
                        logger.debug("epsilon to %s with %s", new_tuple, edge)
                        n += 1
                    else:
                        self.S.append(new_tuple)
                        logger.debug("consume to %s with %s", new_tuple, edge)
                        dig = dst.find_accepted(new_conf)
                        if dig is not None:
                            self.S.append(self.current_tuple(dig))
                            logger.debug("found accepted %s", dig)

        out = Stream()
        lazy_delete = dict()
        for k, conf in self.S:
            if k in lazy_delete:
                continue
            if dst.accept(conf):
                out.append(dst.output(conf))
                logger.debug("accept %s", conf)

                if self.strategy == "NoSkip":
                    pass
                elif self.strategy == "SkipToNext":
                    logger.debug("Prune partial matches started at %d", k)
                    lazy_delete[k] = True
                elif self.strategy == "SkipPastLastEvent":
                    logger.debug("Prune all partial matches")
                    self.S.clear()
                    break
                else:
                    raise ValueError("Unknown strategy: {}".format(self.strategy))
        logger.debug("total out: %s", out)
        return out
