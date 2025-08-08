'''
otacon: Extracts Reddit comments from the offline version of the Pushshift Corpus dumps (see README for further info)

For Usage and Flags see README doc.
'''

import os
import re
import csv
import json
import logging
import calendar
import argparse
from typing import TextIO

from quiet.argument_handling import define_parser, handle_args
from quiet.pushshift_handling import read_redditfile
from quiet.prep_input import establish_timeframe
from quiet.sampling import get_samplepoints

# return stats from which subreddits the relevant comments were and how many per subreddits
stats_dict = {}


def find_all_matches(text, regex):
    """Iterate through all regex matches in a text, yielding the span of each as tuple."""
    
    for match in regex.finditer(text):
        yield (match.start(), match.end())


def inside_quote(text: str, span: tuple) -> bool:
    """
    Test if a span-marked match is inside a quoted line.
    Such lines in Reddit data begin with "&gt;".
    """
    end = span[1]
    relevant_text = text[:end]
    return True if re.search('&gt;[^\n]+$', relevant_text) else False # tests if there is no linebreak between a quote symbol and the match


def extract(args, comment_or_post: dict, compiled_comment_regex: str, include_quoted: bool, outfile: TextIO, filter_reason: str):
    """
    Extract a comment or post text and all relevant metadata.
    If no regex is supplied, extract the whole comment leaving the span field blank.
    If a regex is supplied, extract each match separately with its span info.
    Discard regex matches found inside of a quoted line.
    """
    
    if args.return_all:
        comment_or_post = json.dumps(comment_or_post)
        _=outfile.write(comment_or_post+'\n')
    
    else:
        text = comment_or_post['body'] if args.searchmode == 'comms' else comment_or_post['selftext']
        user = comment_or_post['author']
        flairtext = comment_or_post['author_flair_text']
        subreddit = comment_or_post['subreddit']
        score = comment_or_post['score']
        date = comment_or_post['created_utc']
        
        # assemble a standard Reddit URL for older data
        url_base = "https://www.reddit.com/r/"+subreddit+"/comments/"
        
        oldschool_link = url_base + comment_or_post['link_id'].split("_")[1] + "//" + comment_or_post['id'] if 'link_id' in comment_or_post.keys() else None

        # choose the newer "permalink" metadata instead if available
        permalink = "https://www.reddit.com" + comment_or_post['permalink'] if 'permalink' in comment_or_post.keys() else oldschool_link

        csvwriter = csv.writer(outfile, delimiter=";", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if compiled_comment_regex is None:
            span = None
            row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
            csvwriter.writerow(row)
        else:
            for span in find_all_matches(text, compiled_comment_regex):
                if not include_quoted and not inside_quote(text, span):
                    span = str(span)
                    row = [text, span, subreddit, score, user, flairtext, date, permalink, filter_reason]
                    csvwriter.writerow(row)


def relevant(comment_or_post: dict, args: argparse.Namespace) -> bool:
    """
    Test if a Reddit comment or post is at all relevant to the search.
    This is for broad criteria so negatives are discarded.
    The filters are ordered by how unlikely they are to pass for efficiency.
    """
    # TODO: Reduce to either identification of starting-point comment or identification of parent comment


def log_month(month: str):
    """Send a message to the log with a month's real name for better clarity."""
    month = month.replace("RC_", "")
    month = month.replace("RS_", "")
    month = month.replace(".zst", "")
    year = month.split("-")[0] # get year string from the format 'RC_YYYY-MM.zst'
    m_num = int(month.split("-")[1]) # get month integer
    m_name = calendar.month_name[m_num]

    logging.info("Processing " + m_name + " " + year)


def process_month(month, args, outfile, reviewfile):
    log_month(month)
    relevant_count = 0
    total_count = -1
    infile = args.input + "/" + month
    compiled_comment_regex = re.compile(args.commentregex) if args.commentregex else None

    if args.sample:
        sample_points = get_samplepoints(month, args.sample, args.input)

    if not args.count:
        outf, reviewf = open(outfile, "a", encoding="utf-8"), open(reviewfile, "a", encoding="utf-8")

    for comment_or_post in read_redditfile(infile):
        total_count += 1
        if not args.sample or (args.sample and total_count == sample_points[0]):

            if args.sample:
                del sample_points[0]
                if len(sample_points) == 0:
                    break
            
            if relevant(comment_or_post, args):
                relevant_count += 1
                if not args.count:
                    
                        
                        filtered, reason = filter(comment_or_post, args.popularity) if not args.dont_filter else False, None
                        if not filtered:
                            extract(args, comment_or_post, compiled_comment_regex, args.include_quoted, outf, filter_reason=None)
                        else:
                            extract(args, comment_or_post, compiled_comment_regex, args.include_quoted, reviewf, filter_reason=reason)
    if not args.count:
        outf.close()
        reviewf.close()
    elif args.count:
        return relevant_count


def main():
    logging.basicConfig(level=logging.NOTSET, format='INFO: %(message)s')
    args = handle_args()
    timeframe = establish_timeframe(args.time_from, args.time_to, args.input)
    if args.reverse_order:
        timeframe.reverse()
    logging.info(f"Searching from {timeframe[0]} to {timeframe[-1]}")

    if not args.count:
        args.output = os.path.abspath(args.output)

    # Writing the CSV headers
    if not args.count:
        for month in timeframe:
            process_month(month, args, outfile, reviewfile)
    

if __name__ == "__main__":
    main()