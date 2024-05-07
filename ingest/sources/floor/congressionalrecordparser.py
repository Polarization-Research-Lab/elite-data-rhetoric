'''
Script stolen from Nick Judd's congressional-record package @ https://github.com/unitedstates/congressional-record
- I basically just took what I needed and added the "parse" function at the end which flows with our code setup a lot more nicely
- We also couldn't use the download feature of the original package, which (I **think**) misses any congressional records that dont have the format CREC-{YYYY}-{MM}-{DD}; so instead, we use the congress.gov api to pull records for a particular date (tagged as part of the congressional record), and then download based on whatever links that api call returns

Citation for the congressional-record repo:
Judd, Nicholas, Dan Drinkard, Jeremy Carbaugh, and Lindsay Young. *congressional-record: A parser for the Congressional Record.* Chicago, IL: 2017.

License from the congressional-record repo:
```
Copyright (c) 2015, Nick Judd
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of congressionalrecord2 nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Portions of this software are also subject to the following license:

Copyright (c) 2014, The Sunlight Foundation

All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of cr nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```
'''
from __future__ import absolute_import
from builtins import object
from bs4 import BeautifulSoup
from io import StringIO, BytesIO
import os
from datetime import datetime
import re
import xml.etree.cElementTree as ET
import logging
import itertools

class crItem(object):
    
    def is_break(self,line):
        for pat in self.parent.item_breakers:
            if re.match(pat,line):
                return True

    def is_skip(self,line):
        for pat in self.parent.skip_items:
            if re.match(pat,line):
                return True

    def item_builder(self):
        parent = self.parent
        if parent.lines_remaining == False:
            logging.info("Reached end of document.")
            return
        item_types = parent.item_types
        content = [parent.cur_line]
        # What is this line
        for kind,params in list(item_types.items()):
            for pat in params['patterns']:
                amatch = re.match(pat,parent.cur_line)
                if amatch:
                    self.item['kind'] = kind
                    #if params['special_case']:
                    #    self.item['flag'] = params['condition']
                    #else:
                    #    self.item['flag'] = False
                    if params['speaker_re']:
                        them = amatch.group(params['speaker_group'])
                        self.item['speaker'] = them
                        if them in list(self.parent.speakers.keys()):
                            self.item['speaker_bioguide'] = \
                              self.parent.speakers[them]['bioguideid']
                        else:
                            self.item['speaker_bioguide'] = None
                    else:
                        self.item['speaker'] = params['speaker']
                        self.item['speaker_bioguide'] = None
                    break
            if amatch:
                break
        # OK so now put everything else in with it
        # that doesn't interrupt an item
        # conditional logic for edge cases goes here.
        #if self.item['flag'] == 'emptystr':
        #    pass
        #else:
        for line in parent.the_text:
            if self.is_break(line):
                break
            elif self.is_skip(line):
                pass
            else:
                content.append(line)
        # The original text was split on newline, so ...
        item_text = '\n'.join(content)
        self.item['text'] = item_text
        
                    
            
    def __init__(self,parent):
        self.item = { 'kind':'Unknown',
             'speaker':'Unknown',
             'text':None,
             'turn':-1
           }

        self.parent = parent
        self.item_builder()
        # self.item['text'] = self.find_items(contentiter)


class ParseCRDir(object):
    
    def gen_dir_metadata(self):
        ''' Load up all metadata for this directory
         from the mods file.'''
        with open(self.mods_path,'r') as mods_file:
            self.mods = BeautifulSoup(mods_file,"lxml")
        
    def __init__(self, abspath, **kwargs):
        
        # dir data
        self.cr_dir = abspath
        self.mods_path = os.path.join(self.cr_dir,'mods.xml')
        self.html_path = os.path.join(self.cr_dir,'html')
        self.gen_dir_metadata()


class ParseCRFile(object):
    # Some regex
    re_time = r'^CREC-(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})-.*'
    re_vol = r'^(?P<title>.*); Congressional Record Vol. (?P<vol>[0-9]+), No. (?P<num>[0-9]+)$'
    re_vol_file =   r'^\[Congressional Record Volume (?P<vol>[0-9]+), Number (?P<num>[0-9]+)'\
                    + r' \((?P<wkday>[A-Za-z]+), (?P<month>[A-Za-z]+) (?P<day>[0-9]+), (?P<year>[0-9]{4})\)\]'
    re_chamber =  r'\[(?P<chamber>[A-Za-z\s]+)\]'
    re_pages =  r'\[Page[s]? (?P<pages>[\w\-]+)\]'
    re_trail = r'From the Congressional Record Online'\
      + r' through the Government (Publishing|Printing) Office \[www.gpo.gov\]$'
    re_rollcall =       r'\[Roll(call)?( Vote)? No. \d+.*\]'
    re_recorderstart =  (r'^\s+(?P<start>'
                        + r'(The (assistant )?legislative clerk read as follows)'
                        + r'|(The nomination considered and confirmed is as follows)'
                        + r'|(The (assistant )?legislative clerk)'
                        + r'|(The nomination was confirmed)'
                        + r'|(There being no objection, )'
                        + r'|(The resolution .*?was agreed to.)'
                        + r'|(The preamble was agreed to.)'
                        + r'|(The resolution .*?reads as follows)'
                        + r'|(The assistant editor .*?proceeded to call the roll)'
                        + r'|(The bill clerk proceeded to call the roll.)'
                        + r'|(The bill clerk called the roll.)'
                        + r'|(The motion was agreed to.)'
                        #+ r'|(The Clerk read the resolution, as follows:)'
                        + r'|(The Clerk read (the resolution, )as follows:)'
                        + r'|(The resolution(, with its preamble,)? reads as follows:)'
                        + r'|(The amend(ment|ed).*?(is)? as follows:)'
                        + r'|(Amendment No\. \d+.*?is as follows:)'
                        + r'|(The yeas and nays resulted.*?, as follows:)'
                        + r'|(The yeas and nays were ordered)'
                        + r'|(The result was announced.*?, as follows:)'
                        + r'|(The .*?editor of the Daily Digest)'
                        + r'|(The (assistant )?bill clerk read as follows:)'
                        + r'|(The .*?read as follows:)'
                        + r'|(The text of the.*?is as follows)'
                        + r'|(amended( to read)? as follows:)'
                        + r'|(The material (previously )?referred to (by.*?)?is as follows:)'
                        + r'|(There was no objection)'
                        + r'|(The amendment.*?was agreed to)'
                        + r'|(The motion to table was .*)'
                        + r'|(The question was taken(;|.))'
                        + r'|(The following bills and joint resolutions were introduced.*)'
                        + r'|(The vote was taken by electronic device)'
                        + r'|(A recorded vote was ordered)'
                        #+ r'|()'
                        + r').*')
    # anchored at the end of the line
    re_recorderend =    (r'('
                        + r'(read as follows:)'
                        + r'|(the Record, as follows:)'
                        + r'|(ordered to lie on the table; as follows:)'
                        + r'|(resolutions as follows:)'
                        + r')$')
    # sometimes the recorder says something that is not unique to them but
    # which, in the right context, we take to indicate a recorder comment.
    re_recorder_fuzzy = (r'^\s+(?P<start>'
                        + r'(Pending:)'
                        + r'|(By M(r|s|rs)\. .* \(for .*)'
                        #+ r'|()'
                        + r').*')
    # NCJ's broader version below, tested on one day of the record.
    # works, honest
    re_recorder_ncj = (r'^\s+(?P<start>'
                       + r'(Pending:)'
                       + r'|(By M(r|rs|s|iss)[\.]? [a-zA-Z]+))'
                       )
    re_clerk = r'^\s+(?P<start>The Clerk (read|designated))'
    re_allcaps = r'^ \s*(?!([_=]+|-{3,}))(?P<title>([A-Z]+[^a-z]+))$'
    re_linebreak = r'\s+([_=]+|-{5,})(NOTE|END NOTE)?([_=]+|-{5,})*\s*'
    re_excerpt = r'\s+(_{3,4})'
    re_newpage =   r'\s*\[\[Page \w+\]\]'
    re_timestamp = r'\s+\{time\}\s+\d{4}'

    # Metadata-making functions
    def title_id(self):
        id_num = self.num_titles
        self.num_titles += 1
        return id_num
        
    def make_re_newspeaker(self):
        speaker_list = '|'.join([mbr for mbr in list(self.speakers.keys()) \
        if self.speakers[mbr]['role'] == 'SPEAKING'])
        if len(speaker_list) > 0:
            re_speakers = r'^(\s{1,2}|<bullet>)(?P<name>((' + speaker_list + ')|(((Mr)|(Ms)|(Mrs)|(Miss))\. (([-A-Z\'])(\s)?)+( of [A-Z][a-z]+)?)|(((The ((VICE|ACTING|Acting) )?(PRESIDENT|SPEAKER|CHAIR(MAN)?)( pro tempore)?)|(The PRESIDING OFFICER)|(The CLERK)|(The CHIEF JUSTICE)|(The VICE PRESIDENT)|(Mr\. Counsel [A-Z]+))( \([A-Za-z.\- ]+\))?)))\.'
        else:
            re_speakers = r'^(\s{1,2}|<bullet>)(?P<name>((((Mr)|(Ms)|(Mrs)|(Miss))\. (([-A-Z\'])(\s)?)+( of [A-Z][a-z]+)?)|((The ((VICE|ACTING|Acting) )?(PRESIDENT|SPEAKER|CHAIR(MAN)?)( pro tempore)?)|(The PRESIDING OFFICER)|(The CLERK)|(The CHIEF JUSTICE)|(The VICE PRESIDENT)|(Mr\. Counsel [A-Z]+))( \([A-Za-z.\- ]+\))?))\.'
        return re_speakers
    
    def people_helper(self,tagobject):
        output_dict = {}
        if 'bioguideid' in tagobject.attrs:
            output_dict['bioguideid'] = tagobject['bioguideid']
        elif 'bioGuideId' in tagobject.attrs:
            output_dict['bioguideid'] = tagobject['bioGuideId']
        else:
            output_dict['bioguideid'] = 'None'
        for key in ['chamber','congress','party','state','role']:
            if key in tagobject.attrs:
                output_dict[key] = tagobject[key]
            else:
                output_dict[key] = 'None'
        try:
            output_dict['name_full'] = tagobject.find('name',{'type':'authority-fnf'}).string
        except:
            output_dict['name_full'] = 'None'
        return output_dict
        
    def find_people(self):
        mbrs = self.doc_ref.find_all('congmember')
        if mbrs:
            for mbr in mbrs:
                self.speakers[mbr.find('name',
                                       {'type':'parsed'}).string] = \
                                       self.people_helper(mbr)
    
    def find_related_bills(self):
        related_bills = self.doc_ref.find_all('bill')
        if len(related_bills) > 0:
            self.crdoc['related_bills'] = \
              [bill.attrs for bill in related_bills]

    def find_related_laws(self):
        related_laws = self.doc_ref.find_all('law')
        if len(related_laws) > 0:
            self.crdoc['related_laws'] = \
              [law.attrs for law in related_laws]

    def find_related_usc(self):
        related_usc = self.doc_ref.find_all('uscode')
        if len(related_usc) > 0:
            self.crdoc['related_usc'] = list(
                itertools.chain.from_iterable(
                    [[dict([('title',usc['title'])] +
                        list(sec.attrs.items())) for sec
                        in usc.find_all('section')]
                        for usc in related_usc]
                    )
                )

    def find_related_statute(self):
        related_statute = self.doc_ref.find_all('statuteatlarge')
        if len(related_statute) > 0:
            self.crdoc['related_statute'] = list(
                itertools.chain.from_iterable(
                    [[dict([('volume',st['volume'])] +
                        list(pg.attrs.items())) for pg
                        in st.find_all('pages')]
                        for st in related_statute]
                    )
                )
        
    def date_from_entry(self):
        year, month, day = re.match(self.re_time,self.access_path).group('year','month','day')
        if self.doc_ref.time:
            from_hr,from_min,from_sec = self.doc_ref.time['from'].split(':')
            to_hr,to_min,to_sec = self.doc_ref.time['to'].split(':')
            try:
                self.doc_date = datetime(int(year),int(month),int(day))
                self.doc_start_time = datetime(int(year),int(month),int(day),\
                int(from_hr),int(from_min),int(from_sec))
                self.doc_stop_time = datetime(int(year),int(month),int(day),\
                int(to_hr),int(to_min),int(to_sec))
                self.doc_duration = self.doc_stop_time - self.doc_start_time
            except:
                logging.info('Could not extract a document timestamp.')
    
    # Flow control for metadata generation
    def gen_file_metadata(self):
        # Sometimes the searchtitle has semicolons in it so .split(';') is a nogo
        temp_ref = self.cr_dir.mods.find('accessid', text=self.access_path)
        if temp_ref is None:
            raise RuntimeError("{} doesn't have accessid tag".format(self.access_path))
        self.doc_ref = temp_ref.parent
        matchobj = re.match(self.re_vol, self.doc_ref.searchtitle.string)
        if matchobj:
            self.doc_title, self.cr_vol, self.cr_num = matchobj.group('title','vol','num')
        else:
            logging.warning('{0} yields no title, vol, num'.format(
                self.access_path))
            self.doc_title, self.cr_vol, self.cr_num = \
              'None','Unknown','Unknown'
        self.find_people()
        self.find_related_bills()
        self.find_related_laws()
        self.find_related_usc()
        self.find_related_statute()
        self.date_from_entry()
        self.chamber = self.doc_ref.granuleclass.string
        self.re_newspeaker = self.make_re_newspeaker()
        self.item_types['speech']['patterns'] = [self.re_newspeaker]

    # That's it for metadata. Below deals with content.

    def read_htm_file(self):
        """
        This function updates a self.cur_line
        attribute. So now for each call to the iterator there are two
        pointers to the next line - one for the function,
        and one for the object.

        The purpose of the attribute is to
        give each parsing function a "starting position"
        so that the handshake between functions is easier. Now
        the current (or last) line is tracked in only one place
        and the same way by all object methods.
        """
        self.lines_remaining = True
        with open(self.filepath, 'r') as htm_file:
            htm_lines = htm_file.read()
            htm_text = BeautifulSoup(htm_lines,"lxml")
        text = htm_text.pre.text.split('\n')
        for line in text:
            self.cur_line = line
            yield line
        self.lines_remaining = False
    
    def get_header(self):
        """
        Only after I wrote this did I realize
        how bad things can go when you call
        next() on an iterator instead of treating
        it as a list.

        This code works, though.
        """
        header_in = next(self.the_text)
        if header_in == u'':
            header_in = next(self.the_text)
        match = re.match(self.re_vol_file, header_in)
        if match:
            vol, num, wkday, month, day, year = match.group( \
            'vol','num','wkday','month','day','year')
        else:
            return False
        header_in = next(self.the_text)
        match = re.match(self.re_chamber, header_in)
        if match:
            if match.group('chamber') == 'Extensions of Remarks':
                chamber = 'House'
                extensions = True
            else:
                chamber = match.group('chamber')
                extensions = False
        else:
            return False
        header_in = next(self.the_text)
        match = re.match(self.re_pages, header_in)
        if match:
            pages = match.group('pages')
        else:
            return False
        header_in = next(self.the_text)
        match = re.match(self.re_trail, header_in)
        if match:
            pass
        else:
            return False
        return vol, num, wkday, month, day, year, chamber, pages, extensions

    def write_header(self):
        self.crdoc['id'] = self.access_path
        header = self.get_header()
        if header:
            self.crdoc['header'] = {'vol':header[0],'num':header[1],\
            'wkday':header[2],'month':header[3],'day':header[4],\
            'year':header[5],'chamber':header[6],'pages':header[7],\
            'extension':header[8]}
        self.crdoc['doc_title'] = self.doc_title

    def get_title(self):
        """
        Throw out empty lines
        Parse consecutive title-matching strings into a title str
        Stop on the first line that isn't empty and isn't a title
        Return the title str if it exists.

        We pretty much assume the first title on the page applies
        to everything below it
        """

        title_str = ''
        for line in self.the_text:
            if line == u'':
                pass
            else:
                a_match = re.match(self.re_allcaps, line)
                if a_match:
                    title_str = ' '.join([title_str,a_match.group('title')])
                else:
                    break

        if len(title_str) > 0:
            return title_str.strip()
        else:
            return False

    def write_page(self):
        turn = 0
        itemno = 0
        title = self.get_title()
        the_content = []
        if title:
            self.crdoc['title'] = title
        else:
            self.crdoc['title'] = None
        while self.lines_remaining:
            # while not re.match(self.re_allcaps,self.cur_line):
            try:
                item = crItem(self).item
                if item['kind'] == 'speech':
                    item['turn'] = turn
                    turn += 1
                item['itemno'] = itemno
                itemno += 1
                the_content.append(item)
            except Exception as e:
                logging.warning('{0}'.format(e))
                break

        self.crdoc['content'] = the_content

        logging.debug('Stopped writing {0}. The last line is: {1}'.format(self.access_path,self.cur_line))

    def parse(self):
        """
        Flow control for parsing content.
        """
        self.the_text = self.read_htm_file()
        self.write_header()
        self.write_page()

    """
    This is a dict of line cases.
    In previous versions, these relations were called
    explicitly multiple times in multiple places.

    This way is more extensible and easier to track cases.

    Usage:
    If break_flow == True: <interrupt current item>
    If speaker_re == True: speaker = re.match(line,
                                     <pattern from patterns>).
                                     .group(<speaker_group>)
    else: speaker = <speaker>
    (ALSO -- see line 176 for how speech patterns is populated)
    It has to come after some of the functions because of
    how I want to handle special cases.
    """
    item_types = { 'speech':
                   {'patterns':['Mr. BOEHNER'],
                    'speaker_re':True,
                    'speaker_group':'name',
                    'break_flow':True,
                    'special_case':False
                    },
                    'recorder':
                    {'patterns':[re_recorderstart,
                                 re_recorderend,
                                 re_recorder_ncj],
                    'speaker_re':False,
                    'speaker':'The RECORDER',
                    'break_flow':True,
                    'special_case':False
                    },
                    'clerk':
                    {'patterns':[re_clerk],
                     'speaker_re':False,
                     'speaker':'The Clerk',
                     'break_flow':True,
                     'special_case':False
                     },
                     'linebreak':
                     {'patterns':[re_linebreak],
                      'speaker_re':False,
                      'speaker':'None',
                      'break_flow':True,
                      'special_case':True,
                      'condition':'emptystr'
                      },
                      'excerpt':
                      {'patterns':[re_excerpt],
                       'speaker_re':False,
                       'speaker':'None',
                       'break_flow':True,
                       'special_case':True,
                       'condition':'lastspeaker'
                       },
                      'rollcall':
                      {'patterns':[re_rollcall],
                      'speaker_re':False,
                      'speaker':'None',
                      'break_flow':True,
                      'special_case':False
                      },
                      'metacharacters':
                      {'patterns':[re_timestamp,
                                   re_newpage],
                       'speaker_re':False,
                       'speaker':'None',
                       'break_flow':False,
                       'special_case':False
                       },
                      'empty_line':
                      {'patterns':[r'(^[\s]+$)'],
                       'speaker_re':False,
                       'speaker':'None',
                       'break_flow':False,
                       'special_case':False
                       },
                       'title':
                       {'patterns':[re_allcaps],
                        'speaker_re':False,
                        'speaker':'None',
                        'break_flow':True,
                        'special_case':False,
                        }
                    }

    def __init__(self, abspath, cr_dir, **kwargs):

        # Some metadata
        self.crdoc = {}
        self.crdoc['header'] = False
        self.crdoc['content'] = []
        self.num_titles = 0
        self.speakers = {}
        self.doc_ref = ''
        self.doc_time = -1
        self.doc_start_time = -1
        self.doc_stop_time = -1
        self.doc_duration = -1
        self.doc_chamber = 'Unspecified'
        self.doc_related_bills = []
        
        # file data
        self.filepath = abspath
        self.filedir, self.filename = os.path.split(abspath)
        self.cr_dir = cr_dir
        self.access_path = self.filename.split('.')[0]

        # Generate all metadata including list of speakers
        self.gen_file_metadata()
        # Must come after speaker list generation
        self.item_breakers = []
        self.skip_items = []
        for x in list(self.item_types.values()):
            if x['break_flow'] == True:
                self.item_breakers.extend(x['patterns'])
            else:
                self.skip_items.extend(x['patterns'])

        # Parse the file
        self.parse()

def parse(cr_dir): # this is taken from the downloader.py file of congressional-record
    cr_parser = ParseCRDir(cr_dir)
    results = []
    for file in os.listdir(os.path.join(cr_dir, 'html')):
        parse_path = os.path.join(cr_dir, 'html', file)
        if any((
            '-PgD' in parse_path,
            'FrontMatter' in parse_path,
            '-Pgnull' in parse_path)):
            pass
            # logging.info('Skipping {}'.format(parse_path))
        else:
            crfile = ParseCRFile(parse_path, cr_parser)
            yield crfile
    # return results