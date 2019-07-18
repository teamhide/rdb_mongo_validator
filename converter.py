import abc


class Converter:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _convert_embedded_documents(self, **kwargs):
        pass
