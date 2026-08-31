from euv_acquisition.ecs_logging import FaultTolerantEcsLogger


def test_ecs_logger_failure_does_not_escape_or_repeat_warning(capsys) -> None:
    class BrokenLogger:
        def log(self, _message, **_kwargs) -> None:
            raise ConnectionError("fixture disconnected")

    logger = FaultTolerantEcsLogger(BrokenLogger())

    logger.log("first")
    logger.log("second")

    output = capsys.readouterr().out
    assert output.count("continuing with journald") == 1
    assert "fixture disconnected" in output