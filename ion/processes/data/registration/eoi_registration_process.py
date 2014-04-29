#!/usr/bin/env python
from pyon.ion.process import SimpleProcess
from pyon.ion.event import EventSubscriber
from pyon.public import OT, RT

class EOIRegistrationProcess(SimpleProcess):

    def on_start(self):
        self.data_source_subscriber = EventSubscriber(event_type=OT.ResourceModifiedEvent,
                                                      origin_type=RT.DataSource,
                                                      callback=self._register_data_source)
        self.provider_subscriber = EventSubscriber(event_type=OT.ResourceModifiedEvent,
                                                      origin_type=RT.ExternalDataProvider,
                                                      callback=self._register_provider)
        self.data_source_subscriber.start()
        self.provider_subscriber.start()

    def _register_data_source(self, event, *args, **kwargs):
        print "resource id:", event.origin

    def _register_provider(self, event, *args, **kwargs):
        print "provider id:", event.origin

    def on_quit(self):
        self.data_source_subscriber.stop()
        self.provider_subscriber.stop()

