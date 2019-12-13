# The MIT License (MIT)
# Copyright (c) 2019 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
# of the Software, and to permit persons to whom the Software is furnished to do
# so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import click

__author__ = "Helge Dzierzon (Brockmann Consult GmbH)"



@click.command(name='compute')
@click.argument('script')
@click.argument('cube', nargs=-1)
@click.option('--variables', '--vars', 'input_var_names', metavar='VARIABLES',
              help="Comma-separated list of variable names.")
@click.option('--params', '-p', 'input_params', metavar='PARAMS',
              help="Parameters passed as 'input_params' dict to compute() and init() functions in SCRIPT.")
@click.option('--output', '-o', 'output_path', metavar='OUTPUT', default=DEFAULT_OUTPUT_PATH,
              help=f'Output path. Defaults to {DEFAULT_OUTPUT_PATH!r}')
@click.option('--format', '-f', 'output_format', metavar='FORMAT',
              default='zarr',
              type=click.Choice(['zarr', 'nc']),
              help="Output format.")
@click.option('--name', '-N', 'output_var_name', metavar='NAME',
              default='output',
              help="Output variable's name.")
@click.option('--dtype', '-D', 'output_var_dtype', metavar='DTYPE',
              default='float64',
              type=click.Choice(["uint8", "int8", "uint16", "int16", "uint32", "int32", "float32", "float64"]),
              help="Output variable's data type.")
def create_table():
    """
    Compute a cube from one or more other cubes.
    The command computes a cube variable from other cube variables in CUBEs
    using a user-provided Python function in SCRIPT.
    The SCRIPT must define a function named "compute":
    \b
        def compute(*input_vars: numpy.ndarray,
                    input_params: Mapping[str, Any] = None,
                    dim_coords: Mapping[str, np.ndarray] = None,
                    dim_ranges: Mapping[str, Tuple[int, int]] = None) \\
                    -> numpy.ndarray:
            # Compute new numpy array from inputs
            # output_array = ...
            return output_array
    where input_vars are numpy arrays (chunks) in the order given by VARIABLES or given by the variable names returned
    by an optional "initialize" function that my be defined in SCRIPT too, see below. input_params is a mapping of
    parameter names to values according to PARAMS or the ones returned by the aforesaid "initialize" function.
    dim_coords is a mapping from dimension name to coordinate labels for the current chunk to be computed.
    dim_ranges is a mapping from dimension name to index ranges into coordinate arrays of the cube.
    The SCRIPT may define a function named "initialize":
    \b
        def initialize(input_cubes: Sequence[xr.Dataset],
                       input_var_names: Sequence[str],
                       input_params: Mapping[str, Any]) \\
                       -> Tuple[Sequence[str], Mapping[str, Any]]:
            # Compute new variable names and/or new parameters
            # new_input_var_names = ...
            # new_input_params = ...
            return new_input_var_names, new_input_params
    where input_cubes are the respective CUBEs, input_var_names the respective VARIABLES, and input_params
    are the respective PARAMS. The "initialize" function can be used to validate the data cubes, extract
    the desired variables in desired order and to provide some extra processing parameters passed to the
    "compute" function.
    Note that if no input variable names are specified, no variables are passed to the "compute" function.
    The SCRIPT may also define a function named "finalize":
    \b
        def finalize(output_cube: xr.Dataset,
                     input_params: Mapping[str, Any]) \\
                     -> Optional[xr.Dataset]:
            # Optionally modify output_cube and return it or return None
            return output_cube
    If defined, the "finalize" function will be called before the command writes the
    new cube and then exists. The functions may perform a cleaning up or perform side effects such
    as write the cube to some sink. If the functions returns None, the CLI will *not* write
    any cube data.
    """
    from xcube.cli.common import parse_cli_kwargs
    from xcube.core.compute import compute_cube



