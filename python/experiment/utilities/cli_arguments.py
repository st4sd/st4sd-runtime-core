from six import string_types

def cb_parse_bool(option, opt, value, parser, inverse=False):
    """Callback to use with optparse.OptionParser which converts a string to a boolean and updates option in parser

    API: https://docs.python.org/3/library/optparse.html#optparse-option-callbacks

    Arguments:
       option(optparse.Option): The option item that is parsed
       opt(str): cmdline option name (e.g. --help, -h)
       value(str): value of cmdline option (optional)
       parser(optparse.OptionParser): the parser object
       inverse(bool): Whether to return the inverse of the boolean (Default: False)
    """
    try:
        ret = arg_to_bool(opt, value)
    except (TypeError, ValueError) as e:
        parser.error(e)
    else:
        ret = ret if inverse is False else not ret
        setattr(parser.values, option.dest, ret)


def arg_to_bool(name, val):
    """Converts a str value (positive, negative) to a boolean (case insensitive match)

    Arguments:
        name(str): Name of argument
        val(str): Can be one of the following (yes, no, true, false, y, n)

    Returns
        boolean - True if val is "positive", False if val is "negative

    Raises:
        TypeError - if val is not a string
        ValueError - if val.lower() is not one of [yes, no, true, false, y, n]
    """

    options_positive = ['yes', 'true', 'y']
    options_negative = ['no', 'false', 'n']
    if isinstance(val, string_types) is False:
        raise TypeError("Value of argument %s=%s is not a string but a %s" % (name, val, type(val)))

    val = val.lower()
    if val in options_negative:
        return False
    elif val in options_positive:
        return True
    else:
        raise ValueError("Value of argument %s=%s is not in %s" % (name, val, options_positive + options_negative))