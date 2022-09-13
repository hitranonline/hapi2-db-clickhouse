""" Mappings for SQLite backend of SQLAlchemy """

from hapi2.db.sqlalchemy.base import Column, deferred, PickleType

from hapi2.db.sqlalchemy.base import commit, query

from hapi2.db.sqlalchemy import models

from ....format.dispatch import FormatDispatcher_JSON, FormatDispatcher_Dotpar

from hapi2.config import SETTINGS

from ..updaters import __update_and_commit_core__, __insert_transitions_core__

from clickhouse_sqlalchemy import Table as clhs_Table, make_session as clhs_make_session, \
    get_declarative_base as clhs_get_declarative_base, types as clhs_types, engines as clhs_engines

import warnings
from sqlalchemy import exc as sa_exc
warnings.simplefilter("ignore", category=sa_exc.SAWarning) # ignore warning about columns with same names

#BLOBTYPE = BLOB
BLOBTYPE = clhs_types.String # DEBUG THIS
TEXTTYPE = clhs_types.String
VARCHARTYPE = clhs_types.String
DOUBLETYPE = clhs_types.Float64
INTTYPE = clhs_types.Int32
DATETYPE = clhs_types.Date

INTTYPE8 = clhs_types.Int8
INTTYPE16 = clhs_types.Int16
INTTYPE32 = clhs_types.Int32
INTTYPE64 = clhs_types.Int64

Base = clhs_get_declarative_base()
make_session = lambda engine: clhs_make_session(engine)

#engine_meta = clhs_engines.Memory()
#engine_meta = clhs_engines.MergeTree()
#engine_meta = clhs_engines.MergeTree(order_by='id') # customize sorting for each class??
engine_meta = clhs_engines.ReplacingMergeTree(order_by='id') # customize sorting for each class?? ReplacingMergeTree -> deduplication
IS_UNIQUE = False
IS_NULLABLE = True
Table = clhs_Table
    
#def search_string(query,cls,field,pattern):
#    return query.filter(getattr(cls,field).ilike(pattern+'\0%'))
def search_string(query,cls,field,pattern):
    return query.filter(getattr(cls,field).ilike(pattern))

class CRUD_Generic(models.CRUD_Generic):    

    __format_dispatcher_class__ = FormatDispatcher_JSON

    @classmethod
    def update(cls,header,local=True,**argv):
        tmpdir = SETTINGS['tmpdir']
        cls.__check_types__(header)                   
        stream = cls.__format_dispatcher_class__().getStreamer(basedir=tmpdir,header=header)
        return __update_and_commit_core__(
            cls,stream,cls.__refs__,cls.__backrefs__,local=local,**argv)

class CRUD_Dotpar(models.CRUD_Dotpar):

    __format_dispatcher_class__ = FormatDispatcher_Dotpar
    
    @classmethod
    def update(cls,header,local=True,llst_name='default',**argv):
        tmpdir = SETTINGS['tmpdir'] 
        cls.__check_types__(header)                   
        stream = cls.__format_dispatcher_class__().getStreamer(basedir=tmpdir,header=header)
        return __insert_transitions_core__(cls,stream,local=local,llst_name=llst_name,**argv)

class CrossSectionData(models.CrossSectionData, CRUD_Generic, Base):

    id = Column(INTTYPE,primary_key=True)
    header_id = Column('header_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('cross_section.id')
    __nu__ = Column('__nu__',BLOBTYPE)
    __xsc__ = Column('__xsc__',BLOBTYPE)

    __table_args__ = (
        engine_meta,
    )

class CrossSection(models.CrossSection, CRUD_Generic, Base):

    id = Column(INTTYPE,primary_key=True)
    molecule_alias_id = Column('molecule_alias_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('molecule_alias.id')
    source_alias_id = Column('source_alias_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('source_alias.id')
    numin = Column('numin',DOUBLETYPE)
    numax = Column('numax',DOUBLETYPE)
    npnts = Column('npnts',INTTYPE)
    sigma_max = Column('sigma_max',DOUBLETYPE)
    temperature = Column('temperature',DOUBLETYPE)
    pressure = Column('pressure',DOUBLETYPE)
    resolution = Column('resolution',DOUBLETYPE)
    resolution_units = Column('resolution_units',VARCHARTYPE)
    broadener = Column('broadener',VARCHARTYPE)
    description = Column('description',VARCHARTYPE)
    apodization = Column('apodization',VARCHARTYPE)
    json = Column('json',VARCHARTYPE) # auxiliary field containing non-schema information
    format = Column('format',VARCHARTYPE)
    status = Column('status',VARCHARTYPE)

    # additional HITRANonline-compliant parameters parameters
    filename = Column('filename',VARCHARTYPE,unique=IS_UNIQUE) # HITRANonline filename

    __table_args__ = (
        engine_meta,
        #Index('cross_section__molecule_alias_id', molecule_alias_id),
    )

class CIACrossSectionData(models.CIACrossSectionData, CRUD_Generic, Base):

    id = Column(INTTYPE,primary_key=True)
    header_id = Column('header_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('cross_section.id')
    __nu__ = Column('__nu__',BLOBTYPE)
    __xsc__ = Column('__xsc__',BLOBTYPE)

    __table_args__ = (
        engine_meta,
    )

class CIACrossSection(models.CIACrossSection, CRUD_Generic, Base):

    id = Column(INTTYPE,primary_key=True)
    collision_complex_alias_id = Column('collision_complex_alias_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('collision_complex_alias.id')
    source_alias_id = Column('source_alias_id',INTTYPE,nullable=IS_NULLABLE) # ,ForeignKey('source_alias.id')
    local_ref_id = Column('local_ref_id',INTTYPE)
    numin = Column('numin',DOUBLETYPE)
    numax = Column('numax',DOUBLETYPE)
    npnts = Column('npnts',INTTYPE)
    cia_max = Column('cia_max',DOUBLETYPE)
    temperature = Column('temperature',DOUBLETYPE)
    resolution = Column('resolution',DOUBLETYPE)
    resolution_units = Column('resolution_units',VARCHARTYPE)
    comment = Column('comment',VARCHARTYPE)
    description = Column('description',VARCHARTYPE)
    apodization = Column('apodization',VARCHARTYPE)
    json = Column('json',VARCHARTYPE) # auxiliary field containing non-schema information
    format = Column('format',VARCHARTYPE)
    status = Column('status',VARCHARTYPE)

    # additional HITRANonline-compliant parameters
    #filename = Column('filename',VARCHARTYPE(255),unique=IS_UNIQUE) # HITRANonline filename
    filename = Column('filename',VARCHARTYPE,unique=False) # HITRANonline filename

    __table_args__ = (
        engine_meta,
        #Index('cross_section__collision_complex_id', collision_complex_id),
    )
    
class SourceAlias(models.SourceAlias, CRUD_Generic, Base):

    id = Column(INTTYPE,primary_key=True)
    source_id = Column('source_id',INTTYPE,nullable=True) # ,ForeignKey('source.id')
    alias = Column('alias',VARCHARTYPE,unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )

class Source(models.Source, CRUD_Generic, Base):

    id = Column(INTTYPE,primary_key=True)
    short_alias = Column('short_alias',VARCHARTYPE,nullable=IS_NULLABLE,unique=IS_UNIQUE)
    type = Column('type',VARCHARTYPE)
    authors = Column('authors',TEXTTYPE)
    title = Column('title',TEXTTYPE)
    journal = Column('journal',VARCHARTYPE)
    volume = Column('volume',VARCHARTYPE)
    page_start = Column('page_start',VARCHARTYPE)
    page_end = Column('page_end',VARCHARTYPE)
    year = Column('year',INTTYPE)
    institution = Column('institution',VARCHARTYPE)
    note = Column('note',TEXTTYPE)
    doi = Column('doi',VARCHARTYPE)
    bibcode = Column('bibcode',VARCHARTYPE)
    url = Column('url',TEXTTYPE)

    __table_args__ = (
        engine_meta,
    )

class ParameterMeta(models.ParameterMeta, CRUD_Generic, Base):

    id = Column(INTTYPE, primary_key=True)
    name = Column('name',VARCHARTYPE,unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE)
    description = Column('description',VARCHARTYPE)
    format = Column('format',VARCHARTYPE)
    units = Column('units',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )

linelist_vs_transition = Table('linelist_vs_transition', Base.metadata,
    Column('id', INTTYPE, primary_key=True), # dummy id for clickhouse
    Column('linelist_id', INTTYPE), #, ForeignKey('linelist.id')
    Column('transition_id', INTTYPE), #, ForeignKey('transition.id')
    #Index('linelist_vs_transition__linelist_id','linelist_id'), # fast search for transitions for given linelist
    engine_meta,
)

class Linelist(models.Linelist, CRUD_Generic, Base):

    id = Column('id',INTTYPE,primary_key=True)
    name = Column('name',VARCHARTYPE,unique=IS_UNIQUE,nullable=False)
    description = Column('description',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )

class Transition(models.Transition, CRUD_Dotpar, Base):

    id = Column(INTTYPE,primary_key=True)
    isotopologue_alias_id = Column('isotopologue_alias_id',INTTYPE,nullable=IS_NULLABLE) #,ForeignKey('isotopologue_alias.id')
    molec_id = Column('molec_id',INTTYPE)
    local_iso_id = Column('local_iso_id',INTTYPE)
    nu = Column('nu',DOUBLETYPE)
    sw = Column('sw',DOUBLETYPE)
    a = Column('a',DOUBLETYPE)
    gamma_air = Column('gamma_air',DOUBLETYPE)
    gamma_self = Column('gamma_self',DOUBLETYPE)
    elower = Column('elower',DOUBLETYPE)
    n_air = Column('n_air',DOUBLETYPE)
    delta_air = Column('delta_air',DOUBLETYPE)
    global_upper_quanta = Column('global_upper_quanta',VARCHARTYPE)
    global_lower_quanta = Column('global_lower_quanta',VARCHARTYPE)
    local_upper_quanta = Column('local_upper_quanta',VARCHARTYPE)
    local_lower_quanta = Column('local_lower_quanta',VARCHARTYPE)
    ierr = Column('ierr',VARCHARTYPE)
    iref = Column('iref',VARCHARTYPE)
    line_mixing_flag = Column('line_mixing_flag',VARCHARTYPE)
    gp = Column('gp',INTTYPE)
    gpp = Column('gpp',INTTYPE)
    
    # Simplest solution possible: all "non-standard" parameters are stored 
    # in the main Transition table as a keys of a picklable dictionary.
    #extra = deferred(Column('extra',PickleType,default={}))
    extra = deferred(Column('extra',VARCHARTYPE,default=''))

    __table_args__ = (
        #Index('transition__isotopologue_alias_id', isotopologue_alias_id),
        engine_meta,
    )

class IsotopologueAlias(models.IsotopologueAlias, CRUD_Generic, Base):

    id = Column(INTTYPE, primary_key=True)
    isotopologue_id = Column('isotopologue_id',INTTYPE) # , ForeignKey('molecule.id')
    alias = Column('alias',VARCHARTYPE,unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )

class Isotopologue(models.Isotopologue, CRUD_Generic, Base):
    
    id = Column(INTTYPE,primary_key=True)
    molecule_alias_id = Column('molecule_alias_id',INTTYPE,nullable=IS_NULLABLE) #,ForeignKey('molecule_alias.id')
    isoid = Column('isoid',INTTYPE)
    inchi = Column('inchi',VARCHARTYPE, unique=IS_UNIQUE)
    inchikey = Column('inchikey',VARCHARTYPE, unique=IS_UNIQUE)
    iso_name = Column('iso_name',VARCHARTYPE, unique=IS_UNIQUE)
    iso_name_html = Column('iso_name_html',VARCHARTYPE)
    abundance = Column('abundance',DOUBLETYPE, nullable=True)
    mass = Column('mass',DOUBLETYPE)
    afgl_code = Column('afgl_code',VARCHARTYPE)

    __table_args__ = (
        #Index('isotopologue__molecule_alias_id', molecule_alias_id),
        engine_meta,
    )

molecule_alias_vs_molecule_category = Table('molecule_alias_vs_molecule_category', Base.metadata,
    Column('id', INTTYPE, primary_key=True), # dummy id for clickhouse
    Column('molecule_alias_id', INTTYPE), #, ForeignKey('molecule_alias.id')
    Column('molecule_category_id', INTTYPE), #, ForeignKey('molecule_category.id')
    #Index('molecule_alias_vs_molecule_category__molecule_alias_id','molecule_alias_id'), # fast search for molecule aliases for given category
    engine_meta,
)      

class MoleculeCategory(models.MoleculeCategory, CRUD_Generic, Base):
    
    id = Column(INTTYPE,primary_key=True)
    category = Column('category',VARCHARTYPE,unique=IS_UNIQUE,nullable=IS_NULLABLE)

    __table_args__ = (
        engine_meta,
    )

class MoleculeAlias(models.MoleculeAlias, CRUD_Generic,Base):
    
    id = Column(INTTYPE, primary_key=True)
    molecule_id = Column('molecule_id',INTTYPE,nullable=True) # , ForeignKey('molecule.id')
    alias = Column('alias',VARCHARTYPE,unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )
    
class Molecule(models.Molecule, CRUD_Generic, Base):
    
    id = Column(INTTYPE,primary_key=True)
    common_name = Column('common_name',VARCHARTYPE)
    ordinary_formula = Column('ordinary_formula',VARCHARTYPE)
    ordinary_formula_html = Column('ordinary_formula_html',VARCHARTYPE)
    stoichiometric_formula = Column('stoichiometric_formula',VARCHARTYPE)
    inchi = Column('inchi',VARCHARTYPE)
    inchikey = Column('inchikey',VARCHARTYPE,unique=IS_UNIQUE)
    csid = Column('csid',INTTYPE)

    __table_args__ = (
        engine_meta,
    )

class CollisionComplexAlias(models.CollisionComplexAlias, CRUD_Generic,Base):
    
    id = Column(INTTYPE, primary_key=True)
    collision_complex_id = Column('collision_complex_id',INTTYPE,nullable=True) # , ForeignKey('collision_complex.id')
    alias = Column('alias',VARCHARTYPE,unique=IS_UNIQUE,nullable=IS_NULLABLE)
    type = Column('type',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )

class CollisionComplex(models.CollisionComplex, CRUD_Generic, Base):
    
    id = Column(INTTYPE,primary_key=True)
    chemical_symbol = Column('chemical_symbol',VARCHARTYPE)

    __table_args__ = (
        engine_meta,
    )
