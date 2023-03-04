from abc import ABC, abstractmethod
from footballdashboardsdata.utils.subclassing import get_all_subclasses


class DataSource(ABC):
    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        Get the name of the data source.

        Returns:
            str: _description_
        """

    @abstractmethod
    def impl_get_data(self, **kwargs):
        """
        Get data from the data source.

        Args:
            **kwargs: _description_

        Returns:
            _description_
        """

    @classmethod
    def get_data(cls, data_requester_name: str, **kwargs):
        """
        Get data from the data source.

        Args:
            data_requester_name (str): _description_
            **kwargs: _description_

        Returns:
            _description_
        """
        try:
            subclass = next(
                c
                for c in get_all_subclasses(cls)
                if c.get_name() == data_requester_name
            )
            return subclass().impl_get_data(**kwargs)
        except StopIteration as e:
            raise ValueError(
                f"Invalid data requester name: {data_requester_name}"
            ) from e
