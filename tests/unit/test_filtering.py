"""Unit tests for collection_task.filtering"""

import pytest
from collection_task.filtering import (
    build_redirect_map,
    build_dataset_resource_pairs,
    apply_offset_and_limit,
    build_retired_resources_set,
)


# Test build_redirect_map

def test_build_redirect_map_creates_mapping():
    """Should create a dictionary mapping old-resource to resource"""
    old_resource_entries = [
        {"old-resource": "abc123", "resource": "xyz789"},
        {"old-resource": "def456", "resource": "uvw012"},
    ]

    result = build_redirect_map(old_resource_entries)

    assert result == {
        "abc123": "xyz789",
        "def456": "uvw012",
    }


def test_build_redirect_map_handles_empty_list():
    """Should return empty dict for empty input"""
    result = build_redirect_map([])
    assert result == {}


# Test build_dataset_resource_pairs

def test_build_dataset_resource_pairs_returns_sorted_pairs():
    """Should return sorted (dataset, resource) tuples"""
    dataset_resource_map = {
        "dataset-b": ["resource-2", "resource-1"],
        "dataset-a": ["resource-3"],
    }

    result = build_dataset_resource_pairs(dataset_resource_map)

    assert result == [
        ("dataset-a", "resource-3"),
        ("dataset-b", "resource-1"),
        ("dataset-b", "resource-2"),
    ]


def test_build_dataset_resource_pairs_filters_by_dataset():
    """Should filter to single dataset when specified"""
    dataset_resource_map = {
        "dataset-a": ["resource-1"],
        "dataset-b": ["resource-2"],
    }

    result = build_dataset_resource_pairs(dataset_resource_map, dataset="dataset-a")

    assert result == [("dataset-a", "resource-1")]


def test_build_dataset_resource_pairs_handles_missing_dataset():
    """Should return empty list when dataset not in map"""
    dataset_resource_map = {
        "dataset-a": ["resource-1"],
    }

    result = build_dataset_resource_pairs(dataset_resource_map, dataset="dataset-missing")

    assert result == []


def test_build_dataset_resource_pairs_preserves_duplicates():
    """Should preserve duplicates where resource used in multiple datasets"""
    dataset_resource_map = {
        "dataset-a": ["resource-1"],
        "dataset-b": ["resource-1"],
    }

    result = build_dataset_resource_pairs(dataset_resource_map)

    assert result == [
        ("dataset-a", "resource-1"),
        ("dataset-b", "resource-1"),
    ]


# Test apply_offset_and_limit

def test_apply_offset_and_limit_applies_offset():
    """Should skip first N pairs when offset specified"""
    pairs = [("ds-a", "res-1"), ("ds-a", "res-2"), ("ds-a", "res-3")]

    result = apply_offset_and_limit(pairs, offset=1)

    assert result == [("ds-a", "res-2"), ("ds-a", "res-3")]


def test_apply_offset_and_limit_applies_limit():
    """Should return only first N pairs when limit specified"""
    pairs = [("ds-a", "res-1"), ("ds-a", "res-2"), ("ds-a", "res-3")]

    result = apply_offset_and_limit(pairs, limit=2)

    assert result == [("ds-a", "res-1"), ("ds-a", "res-2")]


def test_apply_offset_and_limit_applies_both():
    """Should apply offset then limit"""
    pairs = [("ds-a", "res-1"), ("ds-a", "res-2"), ("ds-a", "res-3"), ("ds-a", "res-4")]

    result = apply_offset_and_limit(pairs, offset=1, limit=2)

    assert result == [("ds-a", "res-2"), ("ds-a", "res-3")]


def test_apply_offset_and_limit_raises_error_when_offset_too_large():
    """Should raise ValueError when offset exceeds total pairs"""
    pairs = [("ds-a", "res-1"), ("ds-a", "res-2")]

    with pytest.raises(ValueError, match="Offset 5 is beyond the total number"):
        apply_offset_and_limit(pairs, offset=5)


def test_apply_offset_and_limit_logs_dataset_in_error_message(caplog):
    """Should log dataset name when offset too large"""
    pairs = [("ds-a", "res-1")]

    with pytest.raises(ValueError):
        apply_offset_and_limit(pairs, offset=10, dataset="my-dataset")

    # Check that dataset was logged
    assert "Note: Filtering by dataset 'my-dataset'" in caplog.text


def test_apply_offset_and_limit_handles_no_offset_or_limit():
    """Should return all pairs when no offset or limit specified"""
    pairs = [("ds-a", "res-1"), ("ds-a", "res-2")]

    result = apply_offset_and_limit(pairs)

    assert result == pairs


# Test build_retired_resources_set

def test_build_retired_resources_set_filters_status_410():
    """Should return set of resources with status 410"""
    old_resource_entries = [
        {"old-resource": "abc123", "status": "410"},
        {"old-resource": "def456", "status": "200"},
        {"old-resource": "ghi789", "status": "410"},
    ]

    result = build_retired_resources_set(old_resource_entries)

    assert result == {"abc123", "ghi789"}


def test_build_retired_resources_set_handles_missing_status():
    """Should handle entries without status field"""
    old_resource_entries = [
        {"old-resource": "abc123"},
        {"old-resource": "def456", "status": "410"},
    ]

    result = build_retired_resources_set(old_resource_entries)

    assert result == {"def456"}


def test_build_retired_resources_set_handles_empty_list():
    """Should return empty set for empty input"""
    result = build_retired_resources_set([])
    assert result == set()
