#!/usr/bin/env python3
"""Main runner / tiny CLI for the ELT pipelines.

Usage examples:
  python main.py extract weather
  python main.py transform aq
  python main.py load city
  python main.py all
"""
import argparse
import sys
from utils.logging import setup_logging, get_logger
from scripts.extract.crawl_weather import extract_weather
from scripts.extract.crawl_aq import extract_aq
from scripts.extract.crawl_city import crawl_city
from scripts.extract.generate_date import generate_data_date
from scripts.extract.generate_time import generate_data_time
from scripts.transform.transform_weather import transform_weather
from scripts.transform.transform_aq import transform_aq
from scripts.transform.transform_city import transform_city
from scripts.load.load_weather import load_weather
from scripts.load.load_aq import load_aq
from scripts.load.load_city import load_city
from scripts.load.load_date import load_date_DW
from scripts.load.load_time import load_time_db

def _call(func):
	logger = get_logger("main")
	logger.info("→ Running: %s.%s", func.__module__, func.__name__)
	try:
		func()
	except Exception as e:
		logger.exception("Error while running %s: %s", func, e)
		raise


def run_extract(domain: str):
	if domain == "weather":
		_call(extract_weather)
	elif domain == "aq":
		_call(extract_aq)
	elif domain == "city":
		_call(crawl_city)
	elif domain == "date":

		_call(generate_data_date)
	elif domain == "time":

		_call(generate_data_time)
	else:
		raise ValueError("Unknown domain for extract: %s" % domain)


def run_transform(domain: str):
	if domain == "weather":
		

		_call(transform_weather)
	elif domain == "aq":
		

		_call(transform_aq)
	elif domain == "city":
		

		_call(transform_city)
	else:
		raise ValueError("Unknown domain for transform: %s" % domain)


def run_load(domain: str):
	if domain == "weather":
		

		_call(load_weather)
	elif domain == "aq":
		

		_call(load_aq)
	elif domain == "city":
		

		_call(load_city)
	elif domain == "date":
		

		_call(load_date_DW)
	elif domain == "time":
		

		_call(load_time_db)
	else:
		raise ValueError("Unknown domain for load: %s" % domain)


def main():
	# initialize logging early
	setup_logging()
	logger = get_logger("main", domain_file="main.log")
	parser = argparse.ArgumentParser(description="Run ELT steps for this project")
	parser.add_argument("step", choices=["extract", "transform", "load", "all"], help="pipeline step")
	parser.add_argument("domain", nargs="?", choices=["weather", "aq", "city", "date", "time", "all"], default="all", help="dataset domain to run")

	args = parser.parse_args()

	if args.step == "all":
		domains = [args.domain] if args.domain != "all" else ["date", "time", "city", "weather", "aq"]
		for domain in domains:
			run_extract(domain)
			# not all domains have both transform/load (generate/date/time do)
			try:
				run_transform(domain)
			except ValueError:
				pass
			try:
				run_load(domain)
			except ValueError:
				pass
		return

	# run a single step
	domains = [args.domain] if args.domain != "all" else ["date", "time", "city", "weather", "aq"]
	for domain in domains:
		if args.step == "extract":
			run_extract(domain)
		elif args.step == "transform":
			run_transform(domain)
		elif args.step == "load":
			run_load(domain)


if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		logger = get_logger("main")
		logger.exception("Pipeline runner returned an error: %s", e)
		sys.exit(2)

