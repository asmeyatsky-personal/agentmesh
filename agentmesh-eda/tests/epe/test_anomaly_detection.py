from agentmesh.epe.anomaly_detection import AnomalyDetector


def test_anomaly_detector_init():
    detector = AnomalyDetector(window_size=50, std_dev_threshold=2.5)
    assert detector.window_size == 50
    assert detector.std_dev_threshold == 2.5
    assert detector.data_window == []


def test_detect_no_anomaly():
    detector = AnomalyDetector(window_size=5, std_dev_threshold=3)
    for i in range(10):
        assert not detector.detect(10)


def test_detect_anomaly():
    detector = AnomalyDetector(window_size=10, std_dev_threshold=3)
    normal_data = [10] * 10
    detector.train_model(normal_data)
    assert detector.detect(100)  # Anomaly


def test_detect_anomaly_with_negative_values():
    detector = AnomalyDetector(window_size=10, std_dev_threshold=2)
    normal_data = [-5] * 10
    detector.train_model(normal_data)
    assert not detector.detect(-6)
    assert detector.detect(-15)  # Anomaly


def test_train_model():
    detector = AnomalyDetector(window_size=5)
    training_data = [1, 2, 3, 4, 5, 6, 7]
    detector.train_model(training_data)
    assert detector.data_window == [3, 4, 5, 6, 7]
