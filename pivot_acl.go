// Copyright 2019 Hayo van Loon
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"cloud.google.com/go/bigquery"
	"context"
	"flag"
	"fmt"
	"google.golang.org/api/iterator"
	"os"
	"sort"
	"strings"
)

// A type for storing generalised access information
type accessRegister map[string][]resourceAccess

func (ar accessRegister) String() string {
	sb := strings.Builder{}

	var us []string
	for k := range ar {
		us = append(us, k)
	}
	sort.Strings(us)

	for _, u := range us {
		xs, _ := ar[u]
		for _, x := range xs {
			sb.WriteString(u)
			sb.WriteRune('\t')
			sb.WriteString(x.resource)
			sb.WriteRune('\t')
			sb.WriteString(x.access.String())
			sb.WriteRune('\n')
		}
		sb.WriteRune('\n')
	}
	return sb.String()
}

// A type for storing access in Unix-style scheme (rwx)
type access int

type resourceAccess struct {
	resource string
	access   access
}

func stringSingle(a access, n int, c rune) rune {
	if int(a)&n == 0 {
		return '-'
	} else {
		return c
	}
}

func (a access) String() string {
	sb := strings.Builder{}
	sb.WriteRune(stringSingle(a, 1, 'r'))
	sb.WriteRune(stringSingle(a, 2, 'w'))
	sb.WriteRune(stringSingle(a, 4, 'x'))
	return sb.String()
}

func (ra resourceAccess) String() string {
	return fmt.Sprintf("%v: %v", ra.resource, ra.access)
}

// Maps a BigQuery role to an access
var accessMap = map[bigquery.AccessRole]access{
	bigquery.ReaderRole: access(4 + 1),
	bigquery.WriterRole: access(4 + 2 + 1),
	bigquery.OwnerRole:  access(4 + 2 + 1),
}

// my own type: to allow for generalisation across APIs
type myType string

const (
	userEmail  myType = "User"
	groupEmail myType = "Group"
	sgroup     myType = "SpecialGroup"
)

var typeMap = map[bigquery.EntityType]myType{
	bigquery.UserEmailEntity:    userEmail,
	bigquery.GroupEmailEntity:   groupEmail,
	bigquery.SpecialGroupEntity: sgroup,
}

func (t myType) isExpandable() bool {
	return t != userEmail
}

// Updates the access register with the (new) fact
func updateAccessMap(ar accessRegister, a *bigquery.AccessEntry, d string) {
	xs, found := ar[a.Entity]
	if !found {
		xs = []resourceAccess{}
	}

	ac := accessMap[a.Role]

	idx := -1
	for i, x := range xs {
		if x.resource == d {
			idx = i
			break
		}
	}

	if idx == -1 {
		xs = append(xs, resourceAccess{d, access(ac)})
	} else {
		xs[idx] = resourceAccess{d, xs[idx].access | ac}
	}

	ar[a.Entity] = xs
}

// Merges two access registers
func merge(ar1 accessRegister, ar2 accessRegister) accessRegister {
	panic("implement me!")
}

// Retrieves special groups from IAM
func getSpecialGroups() map[string][]string {
	panic("implement me!")
}

// Fetches users and access register for BigQuery
func getBigQueryMeta(projectId string) (map[string]myType, accessRegister, error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return nil, nil, err
	}

	us := make(map[string]myType)
	am := make(map[string][]resourceAccess)

	ds := client.Datasets(ctx)
	ds.ProjectID = projectId

	for {
		d, err := ds.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		m, err := d.Metadata(ctx)
		for _, a := range m.Access {
			t, found := typeMap[a.EntityType]
			if !found {
				continue
			}
			t2, found := us[a.Entity]
			if !found {
				us[a.Entity] = t
			} else if t2 != t {
				return nil, nil, fmt.Errorf("encountered user type mismatch %v <> %v", t, t2)
			}
			updateAccessMap(am, a, d.DatasetID)
		}
	}

	return us, am, nil
}

func main() {
	var projectId string

	flag.StringVar(&projectId, "project_id", os.Getenv("GOOGLE_CLOUD_PROJECT"), "project id")
	flag.Parse()

	if projectId == "" {
		fmt.Println("Please add -project_id <project-id> or set GOOGLE_CLOUD_PROJECT")
		os.Exit(1)
	}

	_, ar, err := getBigQueryMeta(projectId)
	if err != nil {
		panic(err)
	}

	fmt.Print(ar)
}
