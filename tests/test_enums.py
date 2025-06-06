from math import exp
import pytest
from freqtrade.enums import MarketDirection, SignalDirection
from freqtrade0.enums import LongShort  # Update with actual module path


@pytest.mark.parametrize("direction,expected_direction", [
   (SignalDirection.SHORT, LongShort.SHORT),
    (SignalDirection.LONG, LongShort.LONG),
    (MarketDirection.SHORT, LongShort.SHORT),
    (MarketDirection.LONG, LongShort.LONG),
])
def test_signal_direction(direction,expected_direction):
    """Verify conversion from SignalDirection.SHORT"""
    assert LongShort.convert(direction) == expected_direction

# Parameterized test for string inputs
@pytest.mark.parametrize("input_str,expected", [
    ("long", LongShort.LONG),
    ("short", LongShort.SHORT),
    ("*", LongShort.BOTH),
    ("LONG", LongShort.NONE),  # Case-sensitive
    ("", LongShort.NONE),
    ("invalid", LongShort.NONE),
    ("123", LongShort.NONE),
])
def test_string_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert LongShort.convert(input_str) == expected

# Test cases for non-string inputs
def test_none_input():
    """Verify NONE output for None input"""
    assert LongShort.convert(None) == LongShort.NONE

def test_other_types():
    """Verify NONE output for non-string/non-enum inputs"""
    assert LongShort.convert(123) == LongShort.NONE
    assert LongShort.convert(["long"]) == LongShort.NONE
    assert LongShort.convert({"key": "long"}) == LongShort.NONE

# Parameterized test for string inputs
@pytest.mark.parametrize("input_str,expected", [
    (1, LongShort.LONG),
    (2, LongShort.SHORT),
    (LongShort.LONG , LongShort.LONG),
    (LongShort.LONG|LongShort.SHORT, LongShort.BOTH),
 
])
def test_equal_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert input_str == expected

# Parameterized test for string inputs
@pytest.mark.parametrize("input_str,expected", [
    
    (LongShort.LONG, LongShort.BOTH),
    (LongShort.LONG, LongShort.NONE),  # Case-sensitive
    (LongShort.LONG, LongShort.SHORT),
  
])
def test_not_equal_inputs(input_str, expected):
    """Test various string input scenarios"""
    assert input_str != expected