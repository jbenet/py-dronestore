
'''Utility fields mapping Attribute classes to WTForms fields.'''


import json
import wtforms

from dronestore import Attribute, Model
from dronestore import attribute



class JSONField(wtforms.fields.TextAreaField):

  prettyprint_args = { 'sort_keys':True, 'indent':2 }
  def __init__(self, delimiter=',', prettyprint=True, default_value=None, \
      *args, **kwargs):

    self.delimiter = delimiter
    self.prettyprint = prettyprint
    self.default_value = default_value
    super(JSONField, self).__init__(*args, **kwargs)

  def joinPair(self, pair):
    return self.delimiter.join(map(str, list(pair)))

  def _value(self):
    if self.data:
      kwargs = self.prettyprint_args if self.prettyprint else {}
      return unicode(json.dumps(self.data, **kwargs))
    else:
      return unicode(json.dumps(self.default_value))

  def process_formdata(self, valuelist):
    if valuelist:
      self.data = json.loads(str(valuelist[0]))
    else:
      self.data = self.default_value




class ListField(JSONField):

  def __init__(self, *args, **kwargs):
    if 'default_value' not in kwargs:
      kwargs['default_value'] = []
    super(ListField, self).__init__(*args, **kwargs)



class DictField(JSONField):

  def __init__(self, *args, **kwargs):
    if 'default_value' not in kwargs:
      kwargs['default_value'] = {}
    super(DictField, self).__init__(*args, **kwargs)





WTFieldForAttributeMap = {
  attribute.Attribute : wtforms.fields.TextField,
  attribute.TextAttribute : wtforms.fields.TextAreaField,
  attribute.BooleanAttribute : wtforms.fields.BooleanField,
  attribute.IntegerAttribute : wtforms.fields.IntegerField,
  attribute.FloatAttribute : wtforms.fields.FloatField,
  attribute.TimeAttribute : wtforms.fields.DateTimeField,
  attribute.ListAttribute : ListField,
  attribute.DictAttribute : DictField
}




def wtfieldForAttribute(attribute):
  if isinstance(attribute, Attribute):
    attribute = attribute.__class__
  if not issubclass(attribute, Attribute):
    raise ValueError('%s is not an instance of %s' % (attribute, Attribute))

  for cls in attribute.mro():
    if cls in WTFieldForAttributeMap:
      return WTFieldForAttributeMap[cls]

  errstr = '%s passed checkes but did not lead to a valid %s'
  raise ValueError(errstr % (attribute, Attribute))



class ModelForm(wtforms.Form):

  @classmethod
  def patchWithModel(cls, model):
    for attr_name, attr in model.attributes().items():
      if not hasattr(cls, attr_name):
        fieldcls = wtfieldForAttribute(attr)
        setattr(cls, attr_name, fieldcls(label=attr_name))





WTFormForModelMap = {
  Model : ModelForm
}





def wtformForModel(model):
  if isinstance(model, str):
    model = Model.modelNamed(model)
  if isinstance(model, Model):
    model = model.__class__
  if not issubclass(model, Model):
    raise ValueError('%s is not a subclass of %s' % (model, Model))

  if model not in WTFormForModelMap:
    for cls in model.mro():
      if cls in WTFormForModelMap:
        OldForm = WTFormForModelMap[cls]
        class NewForm(OldForm):
          pass
        WTFormForModelMap[model] = NewForm
        break

  form = WTFormForModelMap[model]
  form.patchWithModel(model)

  return form
