//go:build !ignore_autogenerated
// +build !ignore_autogenerated

/*
Copyright 2023 Pramodh Ayyappan.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Role) DeepCopyInto(out *Role) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Role.
func (in *Role) DeepCopy() *Role {
	if in == nil {
		return nil
	}
	out := new(Role)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *Role) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RoleList) DeepCopyInto(out *RoleList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]Role, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RoleList.
func (in *RoleList) DeepCopy() *RoleList {
	if in == nil {
		return nil
	}
	out := new(RoleList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *RoleList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RolePrivilege) DeepCopyInto(out *RolePrivilege) {
	*out = *in
	if in.SuperUser != nil {
		in, out := &in.SuperUser, &out.SuperUser
		*out = new(bool)
		**out = **in
	}
	if in.CreateDb != nil {
		in, out := &in.CreateDb, &out.CreateDb
		*out = new(bool)
		**out = **in
	}
	if in.CreateRole != nil {
		in, out := &in.CreateRole, &out.CreateRole
		*out = new(bool)
		**out = **in
	}
	if in.Login != nil {
		in, out := &in.Login, &out.Login
		*out = new(bool)
		**out = **in
	}
	if in.Inherit != nil {
		in, out := &in.Inherit, &out.Inherit
		*out = new(bool)
		**out = **in
	}
	if in.Replication != nil {
		in, out := &in.Replication, &out.Replication
		*out = new(bool)
		**out = **in
	}
	if in.BypassRls != nil {
		in, out := &in.BypassRls, &out.BypassRls
		*out = new(bool)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RolePrivilege.
func (in *RolePrivilege) DeepCopy() *RolePrivilege {
	if in == nil {
		return nil
	}
	out := new(RolePrivilege)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RoleSpec) DeepCopyInto(out *RoleSpec) {
	*out = *in
	out.ConnectSecretRef = in.ConnectSecretRef
	out.PasswordSecretRef = in.PasswordSecretRef
	if in.DefaultDatabase != nil {
		in, out := &in.DefaultDatabase, &out.DefaultDatabase
		*out = new(string)
		**out = **in
	}
	if in.SSLMode != nil {
		in, out := &in.SSLMode, &out.SSLMode
		*out = new(string)
		**out = **in
	}
	if in.ConnectionLimit != nil {
		in, out := &in.ConnectionLimit, &out.ConnectionLimit
		*out = new(int32)
		**out = **in
	}
	in.Privileges.DeepCopyInto(&out.Privileges)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RoleSpec.
func (in *RoleSpec) DeepCopy() *RoleSpec {
	if in == nil {
		return nil
	}
	out := new(RoleSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RoleStatus) DeepCopyInto(out *RoleStatus) {
	*out = *in
	if in.Conditions != nil {
		in, out := &in.Conditions, &out.Conditions
		*out = make([]v1.Condition, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RoleStatus.
func (in *RoleStatus) DeepCopy() *RoleStatus {
	if in == nil {
		return nil
	}
	out := new(RoleStatus)
	in.DeepCopyInto(out)
	return out
}
