from ..consumer.prefect_consumer import get_arg_parser


def test_arg_parser():
    ""
    args = ["arg_0", "arg_1"]
    arg_parser = get_arg_parser()
    parsed_args = arg_parser.parse_args(args)

    assert parsed_args.endstation == "arg_0"
    assert parsed_args.deployment_name == "arg_1"
    assert parsed_args.kafka-config-file == "/etc/bluesky/kafka.yml"
