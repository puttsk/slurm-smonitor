from .slurm.node import NodeSpecification

__version__ = '0.1.0dev1'
__author__  = 'Putt Sakdhnagool <putt.sakdhnagool@nectec.or.th>'
__license__ = 'MIT'

SERVICE_BEGIN_DATE = '2019-02-21'

NODE_SPECIFICATIONS = {
    'tara-c':NodeSpecification(2, 20),
    'tara-m':NodeSpecification(8, 24),
    'tara-g':NodeSpecification(2, 20)
}
