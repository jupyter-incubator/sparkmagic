from mock import MagicMock

from remotespark.livyclientlib.rawviewer import RawViewer


def test_returns_the_same():
    result = MagicMock()
    viewer = RawViewer()

    returned = viewer.visualize(result)

    assert returned is result
