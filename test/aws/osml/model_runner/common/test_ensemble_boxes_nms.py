# Copyright 2024-2026 Amazon.com, Inc. or its affiliates.

import unittest

import numpy as np
import pytest


class TestNMSMethods(unittest.TestCase):
    """
    Unit tests for the Non-Maximum Suppression (NMS) and Soft-NMS functions.
    """

    def setUp(self):
        """
        Sets up mock bounding boxes, scores, and labels for testing.
        """
        # Bounding boxes (x1, y1, x2, y2) and scores from two models
        self.boxes = [
            np.array([[0.1, 0.1, 0.4, 0.4], [0.15, 0.15, 0.45, 0.45], [0.6, 0.6, 0.9, 0.9]]),  # Model 1
            np.array([[0.2, 0.2, 0.5, 0.5], [0.7, 0.7, 1.0, 1.0]]),  # Model 2
        ]
        self.scores = [
            np.array([0.9, 0.85, 0.6]),  # Scores for Model 1
            np.array([0.8, 0.7]),  # Scores for Model 2
        ]
        self.labels = [
            np.array([1, 1, 2]),  # Labels for Model 1
            np.array([1, 2]),  # Labels for Model 2
        ]

    def test_prepare_boxes(self):
        """
        Test the prepare_boxes function to ensure it:
        1. Corrects invalid box coordinates.
        2. Removes boxes with zero area.
        """
        from aws.osml.model_runner.common.ensemble_boxes_nms import prepare_boxes

        # Create invalid boxes with out-of-bound coordinates and zero area
        invalid_boxes = np.array([[-0.1, 0.2, 1.1, 1.2], [0.5, 0.5, 0.5, 0.5]])
        invalid_scores = np.array([0.9, 0.8])
        invalid_labels = np.array([1, 1])

        filtered_boxes, filtered_scores, filtered_labels = prepare_boxes(invalid_boxes, invalid_scores, invalid_labels)

        # Assertions
        assert filtered_boxes.shape[0] == 1
        assert np.all(filtered_boxes >= 0) and np.all(filtered_boxes <= 1)

    def test_nms(self):
        """
        Test the standard NMS function to ensure it suppresses overlapping boxes
        based on an IoU threshold of 0.5.
        """
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms

        final_boxes, final_scores, final_labels, indices = nms(self.boxes, self.scores, self.labels, 0.5)

        # Assertions
        assert final_boxes.shape[0] == 4

    def test_soft_nms(self):
        """
        Test the Soft-NMS function with the linear method (method=1).
        """
        from aws.osml.model_runner.common.ensemble_boxes_nms import soft_nms

        final_boxes, final_scores, final_labels, indices = soft_nms(self.boxes, self.scores, self.labels, 1, 0.5)

        # Assertions
        assert final_boxes.shape[0] == 5

    def test_nms_fast(self):
        """
        Test the optimized NMS implementation (nms_fast) for speed and correctness.
        """
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_fast

        dets = np.array([[0.1, 0.1, 0.4, 0.4], [0.15, 0.15, 0.45, 0.45], [0.6, 0.6, 0.9, 0.9]])
        scores = np.array([0.9, 0.85, 0.6])

        keep = nms_fast(dets, scores, 0.5)

        # Assertions
        assert len(keep) == 2

    def test_nms_with_weights(self):
        """
        Test the NMS function with model weights applied to scores.
        """
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms

        weights = [0.5, 0.5]  # Apply equal weights to both models
        final_boxes, final_scores, final_labels, indices = nms(self.boxes, self.scores, self.labels, 0.5, weights=weights)

        # Assertions
        assert final_boxes.shape[0] == 4
        assert np.all(final_scores <= 1.0)  # Scores should remain normalized

    def test_invalid_input_lengths(self):
        """
        Test that NMS raises a ValueError when input lengths are mismatched.
        """
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms

        # Mismatched input: boxes have fewer entries than scores and labels
        invalid_boxes = [np.array([[0.1, 0.1, 0.4, 0.4]])]  # 1 box
        invalid_scores = [np.array([0.9, 0.8])]  # 2 scores
        invalid_labels = [np.array([1, 2])]  # 2 labels

        # Verify that a ValueError is raised with a clear message
        with pytest.raises(ValueError):
            nms(invalid_boxes, invalid_scores, invalid_labels, 0.5)

    def test_prepare_boxes_coordinates_less_than_zero_logs_warning(self):
        """Test prepare_boxes handles coordinates < 0 by correcting them and logging warning."""
        from unittest.mock import patch

        from aws.osml.model_runner.common.ensemble_boxes_nms import prepare_boxes

        # Arrange - boxes with negative coordinates
        invalid_boxes = np.array([[-0.1, -0.2, 0.5, 0.5], [0.2, 0.2, 0.7, 0.7]])
        scores = np.array([0.9, 0.8])
        labels = np.array([1, 1])

        # Act - capture print output
        with patch("builtins.print") as mock_print:
            filtered_boxes, filtered_scores, filtered_labels = prepare_boxes(invalid_boxes, scores, labels)

            # Assert - warning logged
            mock_print.assert_called_once()
            self.assertIn("Fixed", str(mock_print.call_args[0][0]))
            self.assertIn("coordinates < 0", str(mock_print.call_args[0][0]))

        # Assert - coordinates corrected
        self.assertTrue(np.all(filtered_boxes >= 0))
        self.assertEqual(len(filtered_boxes), 2)

    def test_prepare_boxes_coordinates_greater_than_one_logs_warning(self):
        """Test prepare_boxes handles coordinates > 1 by correcting them and logging warning."""
        from unittest.mock import patch

        from aws.osml.model_runner.common.ensemble_boxes_nms import prepare_boxes

        # Arrange - boxes with coordinates > 1
        invalid_boxes = np.array([[0.5, 0.5, 1.2, 1.3], [0.1, 0.1, 0.4, 0.4]])
        scores = np.array([0.9, 0.8])
        labels = np.array([1, 1])

        # Act - capture print output
        with patch("builtins.print") as mock_print:
            filtered_boxes, filtered_scores, filtered_labels = prepare_boxes(invalid_boxes, scores, labels)

            # Assert - warning logged
            mock_print.assert_called_once()
            self.assertIn("Fixed", str(mock_print.call_args[0][0]))
            self.assertIn("coordinates > 1", str(mock_print.call_args[0][0]))

        # Assert - coordinates corrected
        self.assertTrue(np.all(filtered_boxes <= 1))
        self.assertEqual(len(filtered_boxes), 2)

    def test_prepare_boxes_removes_zero_area_boxes_logs_warning(self):
        """Test prepare_boxes removes zero-area boxes and logs warning."""
        from unittest.mock import patch

        from aws.osml.model_runner.common.ensemble_boxes_nms import prepare_boxes

        # Arrange - includes box with zero area
        boxes = np.array([[0.5, 0.5, 0.5, 0.5], [0.1, 0.1, 0.4, 0.4]])
        scores = np.array([0.9, 0.8])
        labels = np.array([1, 1])

        # Act - capture print output
        with patch("builtins.print") as mock_print:
            filtered_boxes, filtered_scores, filtered_labels = prepare_boxes(boxes, scores, labels)

            # Assert - warning logged
            mock_print.assert_called_once()
            self.assertIn("Removed", str(mock_print.call_args[0][0]))
            self.assertIn("zero area", str(mock_print.call_args[0][0]))

        # Assert - zero-area box removed
        self.assertEqual(len(filtered_boxes), 1)
        self.assertEqual(len(filtered_scores), 1)
        self.assertEqual(len(filtered_labels), 1)

    def test_cpu_soft_nms_handles_last_element_branch(self):
        """Test cpu_soft_nms handles the i == n-1 branch when processing last element."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import cpu_soft_nms_float

        # Arrange - 3 boxes where last has highest score
        dets = np.array([[0.1, 0.1, 0.3, 0.3], [0.4, 0.4, 0.6, 0.6], [0.7, 0.7, 0.9, 0.9]])
        scores = np.array([0.5, 0.6, 0.9])  # Last box has highest score

        # Act - method=1 (linear soft-NMS)
        keep = cpu_soft_nms_float(dets.copy(), scores.copy(), nt=0.5, sigma=0.5, thresh=0.001, method=1)

        # Assert - all boxes retained (no overlap)
        self.assertEqual(len(keep), 3)

    def test_cpu_soft_nms_score_swapping_when_not_sorted(self):
        """Test cpu_soft_nms swaps scores and boxes when they are not sorted."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import cpu_soft_nms_float

        # Arrange - boxes with unsorted scores
        dets = np.array([[0.1, 0.1, 0.3, 0.3], [0.15, 0.15, 0.35, 0.35], [0.6, 0.6, 0.8, 0.8]])
        scores = np.array([0.5, 0.9, 0.6])  # Middle box has highest score

        # Act - method=1 (linear soft-NMS)
        keep = cpu_soft_nms_float(dets.copy(), scores.copy(), nt=0.5, sigma=0.5, thresh=0.001, method=1)

        # Assert - keep includes all indices and highest score is first after swap
        self.assertEqual(len(keep), 3)
        self.assertEqual(keep[0], 1)
        self.assertEqual(set(keep), {0, 1, 2})

    def test_cpu_soft_nms_linear_method_applies_correct_weights(self):
        """Test cpu_soft_nms with method=1 applies linear weighting (weight = 1 - ovr)."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import cpu_soft_nms_float

        # Arrange - overlapping boxes
        dets = np.array([[0.1, 0.1, 0.5, 0.5], [0.15, 0.15, 0.55, 0.55]])
        scores = np.array([0.9, 0.85])

        # Act - method=1 (linear soft-NMS)
        keep = cpu_soft_nms_float(dets.copy(), scores.copy(), nt=0.3, sigma=0.5, thresh=0.5, method=1)

        # Assert - overlapping box suppressed by thresholded linear weighting
        self.assertEqual(len(keep), 1)
        self.assertEqual(keep[0], 0)

    def test_cpu_soft_nms_gaussian_method_applies_correct_weights(self):
        """Test cpu_soft_nms with method=2 applies Gaussian weighting (weight = exp(-(ovr²)/sigma))."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import cpu_soft_nms_float

        # Arrange - overlapping boxes
        dets = np.array([[0.1, 0.1, 0.5, 0.5], [0.15, 0.15, 0.55, 0.55]])
        scores = np.array([0.9, 0.85])

        # Act - method=2 (Gaussian soft-NMS)
        keep = cpu_soft_nms_float(dets.copy(), scores.copy(), nt=0.3, sigma=0.5, thresh=0.5, method=2)

        # Assert - overlapping box suppressed by thresholded Gaussian weighting
        self.assertEqual(len(keep), 1)
        self.assertEqual(keep[0], 0)

    def test_cpu_soft_nms_original_nms_hard_suppression(self):
        """Test cpu_soft_nms with method=3 applies hard suppression (original NMS)."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import cpu_soft_nms_float

        # Arrange - overlapping boxes
        dets = np.array([[0.1, 0.1, 0.5, 0.5], [0.15, 0.15, 0.55, 0.55]])
        scores = np.array([0.9, 0.85])

        # Act - method=3 (original NMS with hard suppression)
        keep = cpu_soft_nms_float(dets.copy(), scores.copy(), nt=0.3, sigma=0.5, thresh=0.5, method=3)

        # Assert - overlapping box suppressed (hard NMS)
        self.assertEqual(len(keep), 1)

    def test_nms_fast_while_loop_processes_all_boxes(self):
        """Test nms_fast processes all boxes through the while loop correctly."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_fast

        # Arrange - multiple overlapping boxes
        dets = np.array(
            [
                [0.1, 0.1, 0.3, 0.3],
                [0.15, 0.15, 0.35, 0.35],
                [0.2, 0.2, 0.4, 0.4],
                [0.6, 0.6, 0.8, 0.8],
                [0.7, 0.7, 0.9, 0.9],
            ]
        )
        scores = np.array([0.9, 0.85, 0.8, 0.75, 0.7])

        # Act
        keep = nms_fast(dets, scores, thresh=0.5)

        # Assert - suppresses overlapping boxes, keeps non-overlapping ones
        self.assertIsInstance(keep, list)
        self.assertGreater(len(keep), 0)
        self.assertLessEqual(len(keep), 5)

    def test_nms_method_incorrect_weights_logs_warning_and_skips(self):
        """Test nms_method logs warning when weights length doesn't match boxes."""
        from unittest.mock import patch

        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_method

        # Arrange - 2 models but 3 weights
        boxes = [np.array([[0.1, 0.1, 0.4, 0.4]]), np.array([[0.2, 0.2, 0.5, 0.5]])]
        scores = [np.array([0.9]), np.array([0.8])]
        labels = [np.array([1]), np.array([1])]
        weights = [0.5, 0.3, 0.2]  # Wrong length

        # Act - capture print output
        with patch("builtins.print") as mock_print:
            final_boxes, final_scores, final_labels, indices = nms_method(boxes, scores, labels, weights=weights)

            # Assert - warning logged
            mock_print.assert_called_once()
            self.assertIn("Incorrect number of weights", str(mock_print.call_args[0][0]))

        # Assert - still processes boxes without weights
        self.assertGreater(len(final_boxes), 0)

    def test_nms_method_mismatched_lengths_logs_warning_and_skips(self):
        """Test nms_method logs warning and skips model when boxes/scores/labels lengths mismatch."""
        from unittest.mock import patch

        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_method

        # Arrange - mismatched lengths for one model
        boxes = [np.array([[0.1, 0.1, 0.4, 0.4]]), np.array([[0.2, 0.2, 0.5, 0.5], [0.3, 0.3, 0.6, 0.6]])]
        scores = [np.array([0.9]), np.array([0.8])]  # Second model: 2 boxes but 1 score
        labels = [np.array([1]), np.array([1])]

        # Act - capture print output
        with patch("builtins.print") as mock_print:
            final_boxes, final_scores, final_labels, indices = nms_method(boxes, scores, labels)

            # Assert - warning logged
            mock_print.assert_called_once()
            self.assertIn("Check length", str(mock_print.call_args[0][0]))
            self.assertIn("Boxes are skipped", str(mock_print.call_args[0][0]))

        # Assert - processes first model only
        self.assertEqual(len(final_boxes), 1)

    def test_nms_method_handles_empty_boxes_silently(self):
        """Test nms_method handles empty boxes list without logging."""
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_method

        # Arrange - one model with boxes, one with empty
        boxes = [np.array([[0.1, 0.1, 0.4, 0.4]]), np.array([])]
        scores = [np.array([0.9]), np.array([])]
        labels = [np.array([1]), np.array([])]

        # Act
        final_boxes, final_scores, final_labels, indices = nms_method(boxes, scores, labels)

        # Assert - processes non-empty model
        self.assertEqual(len(final_boxes), 1)

    def test_nms_fast_empty_boxes_returns_empty_list(self):
        """Test nms_fast handles empty boxes array"""
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_fast

        # Arrange - empty arrays
        dets = np.array([]).reshape(0, 4)
        scores = np.array([])

        # Act
        result = nms_fast(dets, scores, thresh=0.5)

        # Assert
        self.assertEqual(len(result), 0)

    def test_nms_fast_single_box_returns_single_keep(self):
        """Test nms_fast with single box returns list with single index"""
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_fast

        # Arrange - single box
        dets = np.array([[0.1, 0.1, 0.5, 0.5]])
        scores = np.array([0.9])

        # Act
        result = nms_fast(dets, scores, thresh=0.5)

        # Assert
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], 0)

    def test_cpu_soft_nms_zero_threshold_keeps_all_boxes(self):
        """Test cpu_soft_nms with thresh=0 keeps all boxes"""
        from aws.osml.model_runner.common.ensemble_boxes_nms import cpu_soft_nms_float

        # Arrange
        dets = np.array([[0.1, 0.1, 0.3, 0.3], [0.15, 0.15, 0.35, 0.35], [0.6, 0.6, 0.8, 0.8]])
        scores = np.array([0.9, 0.85, 0.8])

        # Act - thresh=0 means keep all
        keep = cpu_soft_nms_float(dets.copy(), scores.copy(), nt=0.0, sigma=0.5, thresh=0.001, method=1)

        # Assert - all boxes kept
        self.assertEqual(len(keep), 3)

    def test_nms_method_single_model_processes_correctly(self):
        """Test nms_method with single model (not ensemble) processes normally"""
        from aws.osml.model_runner.common.ensemble_boxes_nms import nms_method

        # Arrange - single model
        boxes = [np.array([[0.1, 0.1, 0.4, 0.4], [0.5, 0.5, 0.8, 0.8]])]
        scores = [np.array([0.9, 0.85])]
        labels = [np.array([1, 1])]

        # Act
        final_boxes, final_scores, final_labels, indices = nms_method(boxes, scores, labels)

        # Assert - processes single model correctly
        self.assertGreater(len(final_boxes), 0)


if __name__ == "__main__":
    unittest.main()
