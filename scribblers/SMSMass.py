# Tai Sakuma <tai.sakuma@cern.ch>

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

        smsname =  '_'.join(event.component.name.split('_')[0:2])
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
