from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class CountTest(unittest.TestCase):

  def test_count(self):
    # Our static input data, which will make up the initial PCollection.
    test_output = [
          {
              'date': '2009-01-09 02:54:25 UTC', 'total_transaction_amount': 1021101.99
          },
          {
              'date': '2017-01-01 04:22:23 UTC', 'total_transaction_amount': 19.95
          },
          {
              'date': '2017-03-18 14:09:16 UTC', 'total_transaction_amount': 2102.22
          },
          {
              'date': '2017-03-18 14:10:44 UTC', 'total_transaction_amount': 1.0003
          },
          {
              'date': '2017-03-18 14:10:44 UTC', 'total_transaction_amount': 13700000023.08
          },
          {
              'date': '2018-02-27 16:04:11 UTC', 'total_transaction_amount': 129.12
          }
      ]
      
      expected_results = [
           ('2009-01-09 02:54:25 UTC', 1021101.99),
           ('2017-03-18 14:09:16 UTC', 2102.22),
           ('2017-03-18 14:10:44 UTC', 13700000023.08),
           ('2018-02-27 16:04:11 UTC', 129.12)
           ]
      
      with TestPipeline() as p:
            
           input = p | beam.Create(test_output)
           
           output = input | beam.Filter(has_amount, 20)
           
           assert that(output, equal_to(expected_results))