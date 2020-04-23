import io
import json

import pytest

from coworks import BizFactory
from coworks.cli.sfn import StepFunctionWriter, TechState, add_actions
from coworks.cli.writer import WriterError
from .tech_ms import S3MockTechMS


class TechMS(S3MockTechMS):

    def get_test(self):
        return "get"

    def get_params(self, value, other):
        return f"get {value} and {other}"

    def get_params_(self, value=1, other=2):
        return f"get {value} and {other}"

    def post_params(self, value=1, other=2):
        return f"get {value} and {other}"


def test_no_params():
    tech = TechMS()

    data = {'get': '/test'}
    call = TechState.get_call_data(None, data)
    res = tech(call, {})
    assert res['statusCode'] == 200


def test_arg_params():
    tech = TechMS()

    uri_params = {'_0': 1, '_1': 2}
    data = {'get': '/params/{_0}/{_1}', 'uri_params': uri_params}
    call = TechState.get_call_data(None, data)
    res = tech(call, {})
    assert res['statusCode'] == 200
    assert res['body'] == "get 1 and 2"


def test_kwargs_params():
    tech = TechMS()

    data = {'get': '/params'}
    call = TechState.get_call_data(None, data)
    res = tech(call, {})
    assert res['statusCode'] == 200
    assert res['body'] == "get 1 and 2"

    query_params = {'value': [3], 'other': [4]}
    data = {'get': '/params', 'query_params': query_params}
    call = TechState.get_call_data(None, data)
    res = tech(call, {})
    assert res['statusCode'] == 200
    assert res['body'] == "get 3 and 4"

    query_params = {'value': [5], 'other': [6]}
    data = {'post': '/params', 'query_params': query_params}
    call = TechState.get_call_data(None, data)
    res = tech(call, {})
    assert res['statusCode'] == 200
    assert res['body'] == "get 5 and 6"

    body = {'value': 7, 'other': 8}
    data = {'post': '/params', 'body': body}
    call = TechState.get_call_data(None, data)
    res = tech(call, {})
    assert res['statusCode'] == 200
    assert res['body'] == "get 7 and 8"


def test_biz_empty():
    biz = BizFactory()
    biz.create('test/biz/empty', 'test')
    writer = StepFunctionWriter(biz)
    output = io.StringIO()
    with pytest.raises(WriterError):
        writer.export(output=output, error=output)
    output.seek(0)
    res = output.read()
    assert res == "Error in test/biz/empty: The content of the test/biz/empty microservice seems to be empty.\n"


def test_biz_complete():
    """Tests the doc example."""
    biz = BizFactory()
    biz.create('test/biz/complete', 'test')
    writer = StepFunctionWriter(biz)
    output = io.StringIO()
    writer.export(output=output, error=output)
    output.seek(0)
    source = json.loads(output.read())
    assert source['Version'] == "1.0"
    assert 'Comment' in source
    assert len(source['States']) == 5

    states = source['States']
    data = states['Init']['Result']
    print(data)

    state = states['Check server']
    assert state is not None
    assert state['Type'] == 'Task'

    state = states['Send mail']
    assert state is not None
    assert state['Type'] == 'Task'
    assert state['End'] == True


def test_fail():
    states = []
    actions = [{
        'name': "fail",
        'fail': None
    }]
    add_actions(states, actions)
    assert len(states) == 1
    assert 'End' not in states[0].state


@pytest.mark.wip
def test_pass():
    # missing key cases
    states = []
    actions = [{
        'name': "action",
        'pass': None
    }]
    add_actions(states, actions)
    assert len(states) == 1
    assert 'End' in states[0].state


def test_tech():
    # missing key cases
    states = []
    actions = [{
        'name': "action",
        'tech': {
            'service': "tech",
        }
    }]
    with pytest.raises(WriterError):
        add_actions(states, actions)
    actions = [{
        'name': "action",
        'tech': {
            'get': "/",
        }
    }]
    with pytest.raises(WriterError):
        add_actions(states, actions)

    # normal usage
    states = []
    actions = [{
        'name': "action",
        'tech': {
            'service': "tech",
            'get': "/",
        }
    }]
    add_actions(states, actions)
    assert len(states) == 1
    assert 'End' in states[0].state

    states = []
    actions = [{
        'name': "action 1",
        'tech': {
            'service': "tech",
            'get': "/",
        }
    }, {
        'name': "action 2",
        'tech': {
            'service': "tech",
            'get': "/",
        }
    }]
    add_actions(states, actions)
    print(states)
    assert 'Next' in states[0].state
    assert states[0].state['Next'] == states[1].name
    assert 'End' in states[1].state
