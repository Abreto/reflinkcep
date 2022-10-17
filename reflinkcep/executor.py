import logging

from reflinkcep.DST import DST, Configuration
from reflinkcep.event import Event, EventStream, Stream

Match = dict[str, EventStream]

logger = logging.getLogger(__name__)


class Executor:
    def __init__(self, dst: DST) -> None:
        self.dst = dst

    def reset(self):
        self.S = []
        self.i = 0

    def current_tuple(self, conf: Configuration) -> tuple[int, Configuration]:
        return tuple((self.i, conf))

    def feed(self, event: Event) -> Stream[Match]:
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
            logger.debug("At %s", conf)
            # print(k, i, n, sep=" ")
            i += 1

            q = conf.get_state()
            for edge in dst.start_from(q):
                if edge.predict(conf, event):
                    new_conf = edge.advance(conf, event)
                    new_tuple = self.current_tuple(new_conf)
                    if edge.is_epsilon():
                        T.append(new_tuple)
                        n += 1
                    else:
                        self.S.append(new_tuple)

        out = Stream()
        for k, conf in self.S:
            if dst.accept(conf):
                out.append(dst.output(conf))
            # TODO: after-match strategy
        return out
