from mrjob.job import MRJob

from mrjob.step import MRStep

from mrjob.protocol import BytesValueProtocol

 

class CleanAndNumberBooks(MRJob):
       def steps(self):
         return [

            MRStep(mapper=self.mapper, reducer=self.reducer),

        ]


 

def configure_args(self):

    super(CleanAndNumberBooks, self).configure_args()

    self.add_passthru_arg('--output-encoding', default='utf-8')

 

def mapper(self, _, line):

        # Diviser la ligne en champs

        fields = line.strip().split()

 

        # Vérifier si l'auteur, la langue originale ou le titre original est inconnu

        if 'inconnu' not in fields[1] and 'inconnu' not in fields[2]:

            # Si la langue originale est le français, enlever la partie titre original

            if fields[2] != 'français':

                yield None, line

            else:

                cleaned_line = '\t'.join(fields[:2] + [fields[3]])

                yield None, cleaned_line

 

def reducer(self, _, lines):

        book_number = 0

        for line in lines:

            book_number += 1

            yield book_number, line

 
if __name__ == '__main__':
          CleanAndNumberBooks.run()