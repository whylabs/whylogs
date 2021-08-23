
import json
import pytest

from whylogs.proto import Op
from whylogs.app.config import load_config
from whylogs.app.session import session_from_config
from whylogs.core.statistics.constraints import DatasetConstraints
from whylogs.core.statistics.constraints import SummaryConstraint, ValueConstraint, Op 
from whylogs.util.protobuf import message_to_json
from whylogs.core.statistics.constraints import _value_funcs,_summary_funcs1
def test_value_summary_serialization():
   
    for each_op,_ in _value_funcs.items():

        value = ValueConstraint(each_op, 3.6)
        msg_value= value.to_protobuf()
        json_value=json.loads(message_to_json(msg_value))
        assert json_value["name"]=="value "+Op.Name(each_op)+" 3.6"
        assert pytest.approx(json_value["value"],0.001) == 3.6
        assert json_value["op"] ==Op.Name(each_op)
        assert json_value["verbose"] == False

    for each_op, _ in _summary_funcs1.items():
    # constraints may have an optional name
        sum_constraint = SummaryConstraint('min', each_op, 300000, name='< 30K')
        msg_sum_const=sum_constraint.to_protobuf()
        json_summary= json.loads(message_to_json(msg_sum_const))
    
        assert json_summary["name"]=='< 30K'
        assert pytest.approx(json_summary["value"],0.1) == 300000
        assert json_summary["firstField"] == "min"
        assert json_summary["op"] == str(Op.Name(each_op))
        assert json_summary["verbose"] == False


def test_value_constraints(df_lending_club,local_config_path):

    conforming_loan = ValueConstraint(Op.LT, 548250)
    smallest_loan = ValueConstraint(Op.GT, 2500.0, verbose=True)

    high_fico = ValueConstraint(Op.GT, 4000)

    dc = DatasetConstraints(None, 
        value_constraints={'loan_amnt':[conforming_loan, smallest_loan],
                        'fico_range_high':[high_fico]})
    

    config = load_config(local_config_path)
    session = session_from_config(config)
    

    profile = session.log_dataframe(df_lending_club, 'test.data', constraints=dc)
    session.close()
    report = dc.report()

    assert len(report) == 2
    print(report)
    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1]==50

    assert report[1][1][0][2]==50

def test_summary_constraints(df_lending_club,local_config_path):


    non_negative = SummaryConstraint('min', Op.GE, 0)

    dc = DatasetConstraints(None,
        summary_constraints= { 'annual_inc':[non_negative]})
    config = load_config(local_config_path)
    session = session_from_config(config)
    profile = session.log_dataframe(df_lending_club, 'test.data', constraints=dc)
    session.close()
    report = r = profile.apply_summary_constraints()

    assert len(report) == 1
    # make sure it checked every value
    for each_feat in report:
        for each_constraint in each_feat[1]:
            assert each_constraint[1]==1

