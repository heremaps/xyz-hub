The `enhancer_samples` directory support side-by-side testing of Activity Log.<br>
This is to enable developer with possibility to analyze activity log produced from two features:
- new one (that resides in `new_feature.json`)
- old one (that resides in`old_feature.json`)

Developer also needs to define the expected feature containing `ActivityLog` (`@ns:com:here:xyz:log` namespace that can be found under feature's `properties`).
This is basically the output of the enhancement operation that takes two aforementioned features and creates the enhanced one (this happens in `ActivityLogEnhancer` class).

The expected feature (enhancement output) should be defined in `expected_enhanced_feature.json`.

The actual test implementation (where one can actually put a debug breakpoint on) is in `ActivityLogEnhancerTest`.

