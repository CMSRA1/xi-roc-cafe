# Tai Sakuma <tai.sakuma@cern.ch>

##__________________________________________________________________||
class ComponentName(object):

    def __repr__(self):
        return '{}()'.format(
            self.__class__.__name__,
        )

    def begin(self, event):
        self.vals = [ ]
        event.componentName = self.vals

        self.vals[:] = [event.component.name]
        # e.g., "HTMHT_Run2015D_PromptReco_25ns"

    def event(self, event):
        event.componentName = self.vals

##__________________________________________________________________||
class SMSMass(object):
    def begin(self, event):

        massdict = {
            'SMS_T1tttt': ('GenSusyMGluino', 'GenSusyMNeutralino'),
            'SMS_T1bbbb': ('GenSusyMGluino', 'GenSusyMNeutralino'),
            'SMS_T1qqqq': ('GenSusyMGluino', 'GenSusyMNeutralino'),
            'SMS_T2tt': ('GenSusyMStop', 'GenSusyMNeutralino'),
            'SMS_T2bb': ('GenSusyMSbottom', 'GenSusyMNeutralino'),
            'SMS_T2qq': ('GenSusyMSquark', 'GenSusyMNeutralino'),
        }

        smsname =  '_'.join(event.componentName[0].split('_')[0:2])
        # e.g., 'SMS_T1tttt'

        self.sms = smsname in massdict

        if not self.sms: return

        self.mass1, self.mass2 = massdict[smsname]
        self._attach_to_event(event)

    def _attach_to_event(self, event):
        event.smsmass1 = getattr(event, self.mass1)
        event.smsmass2 = getattr(event, self.mass2)

    def event(self, event):
        if not self.sms: return
        self._attach_to_event(event)

##__________________________________________________________________||
