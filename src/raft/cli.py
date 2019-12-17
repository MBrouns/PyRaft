import logging
import click


@click.group()
@click.option('-v', '--verbose', is_flag=True)
def main(verbose):
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level)


if __name__ == "__main__":
    main()
